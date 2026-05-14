/*
 * Asterisk -- An open source telephony toolkit.
 *
 * Copyright (C) 2026, Asterisk
 *
 * See http://www.asterisk.org for more information about
 * the Asterisk project.
 *
 * This program is free software, distributed under the terms of
 * the GNU General Public License Version 2. See the LICENSE file
 * at the top of the source tree.
 */

/*! \file
 *
 * \brief Kafka integration resource
 */

/*** MODULEINFO
	<depend>rdkafka</depend>
	<support_level>extended</support_level>
 ***/

#include "asterisk.h"

#include <ctype.h>
#include <errno.h>
#include <librdkafka/rdkafka.h>
#include <regex.h>
#include <time.h>

#include "asterisk/config.h"
#include "asterisk/json.h"
#include "asterisk/lock.h"
#include "asterisk/logger.h"
#include "asterisk/manager.h"
#include "asterisk/module.h"
#include "asterisk/taskprocessor.h"
#include "asterisk/utils.h"

#define CONFIG_FILE "res_kafka.conf"
#define PRODUCER_POLL_INTERVAL_SEC 1
#define CREATE_TOPIC_TIMEOUT_MS 2000
#define KAFKA_OVERRIDE_KEY_HEADER "res_kafka_key"
#define KAFKA_OVERRIDE_TOPIC_HEADER "res_kafka_topic"

struct kafka_broker {
	AST_LIST_ENTRY(kafka_broker) entry;
	char *name;
	char *bootstrap_servers;
	struct ast_variable *config;
	ast_mutex_t lock;       /*!< protects rk */
	rd_kafka_t *rk;
	struct ast_taskprocessor *tps;
};

struct kafka_eventfilter {
	AST_LIST_ENTRY(kafka_eventfilter) entry;
	char *name;
	char *value;
	char action[16];
	char event_name[64];
	char header[64];
	char method[32];
	const char *match_expression;
	regex_t regex;
	unsigned int has_regex:1;
	unsigned int is_advanced:1;
	unsigned int is_exclude:1;
};

struct kafka_producer {
	AST_LIST_ENTRY(kafka_producer) entry;
	char *name;
	char *broker;
	char *topic;
	char *key;
	AST_LIST_HEAD_NOLOCK(, kafka_eventfilter) eventfilters;
	unsigned int eventfilter_count;
	struct ast_variable *fields;    /*!< extra fields appended to every JSON message */
	unsigned int create_topic:1;
	unsigned int format_raw:1;      /*!< send the raw AMI event body instead of JSON */
	int topic_partitions;
	int topic_replication_factor;
};

struct kafka_consumer {
	AST_LIST_ENTRY(kafka_consumer) entry;
	char *name;
	char *broker_name;
	char *topic;
	char *group_id;
	struct ast_variable *config;    /*!< extra librdkafka options (enable.auto.commit etc.) */
	rd_kafka_t *rk;
	pthread_t thread;
	char *reply_topic;              /*!< topic to produce AMI responses to (optional) */
	volatile int running;
	unsigned int create_topic:1;
	int topic_partitions;
	int topic_replication_factor;
};

static AST_LIST_HEAD_STATIC(brokers, kafka_broker);
static AST_LIST_HEAD_STATIC(producers, kafka_producer);
static AST_LIST_HEAD_STATIC(consumers, kafka_consumer);

static ast_mutex_t producer_poll_lock;
static ast_cond_t producer_poll_cond;
static pthread_t producer_poll_thread = AST_PTHREADT_NULL;
static unsigned int producer_poll_stop;

static unsigned int module_enabled = 1;
static unsigned int init_without_kafka = 1;
static unsigned int hooks_registered;
static int messages_lost_no_producer;
static int messages_lost_produce_failed;

static void broker_destroy_client(struct kafka_broker *broker)
{
	rd_kafka_t *rk;

	ast_mutex_lock(&broker->lock);
	rk = broker->rk;
	broker->rk = NULL;
	ast_mutex_unlock(&broker->lock);

	if (rk) {
		rd_kafka_flush(rk, 5000);
		rd_kafka_destroy(rk);
	}
}

static void broker_free(struct kafka_broker *broker)
{
	if (!broker) {
		return;
	}

	/* Drain the taskprocessor before destroying rk: tasks in flight may still use it. */
	ast_taskprocessor_unreference(broker->tps);
	broker_destroy_client(broker);
	ast_mutex_destroy(&broker->lock);
	ast_variables_destroy(broker->config);
	ast_free(broker->bootstrap_servers);
	ast_free(broker->name);
	ast_free(broker);
}

static void producer_free(struct kafka_producer *producer)
{
	struct kafka_eventfilter *filter;

	if (!producer) {
		return;
	}

	while ((filter = AST_LIST_REMOVE_HEAD(&producer->eventfilters, entry))) {
		if (filter->has_regex) {
			regfree(&filter->regex);
		}
		ast_free(filter->name);
		ast_free(filter->value);
		ast_free(filter);
	}

	ast_variables_destroy(producer->fields);
	ast_free(producer->key);
	ast_free(producer->topic);
	ast_free(producer->broker);
	ast_free(producer->name);
	ast_free(producer);
}

static int append_broker_config_var(struct kafka_broker *broker, const char *name,
	const char *value)
{
	struct ast_variable *var;

	var = ast_variable_new(name, value, "");
	if (!var) {
		return -1;
	}

	if (!broker->config) {
		broker->config = var;
	} else {
		struct ast_variable *tail;

		for (tail = broker->config; tail->next; tail = tail->next) {
		}
		tail->next = var;
	}

	return 0;
}

static void free_brokers(void)
{
	struct kafka_broker *broker;
	AST_LIST_HEAD_NOLOCK(, kafka_broker) tmp;

	AST_LIST_HEAD_INIT_NOLOCK(&tmp);

	AST_LIST_LOCK(&brokers);
	while ((broker = AST_LIST_REMOVE_HEAD(&brokers, entry))) {
		AST_LIST_INSERT_TAIL(&tmp, broker, entry);
	}
	AST_LIST_UNLOCK(&brokers);

	/* broker_free drains tps, which may lock &brokers — must be outside the list lock. */
	while ((broker = AST_LIST_REMOVE_HEAD(&tmp, entry))) {
		broker_free(broker);
	}
}

static void free_producers(void)
{
	struct kafka_producer *producer;

	AST_LIST_LOCK(&producers);
	while ((producer = AST_LIST_REMOVE_HEAD(&producers, entry))) {
		producer_free(producer);
	}
	AST_LIST_UNLOCK(&producers);
}

static void consumer_free(struct kafka_consumer *consumer)
{
	if (!consumer) {
		return;
	}
	/* rk must be closed/destroyed by the consumer thread before calling this. */
	if (consumer->rk) {
		rd_kafka_destroy(consumer->rk);
	}
	ast_variables_destroy(consumer->config);
	ast_free(consumer->reply_topic);
	ast_free(consumer->group_id);
	ast_free(consumer->topic);
	ast_free(consumer->broker_name);
	ast_free(consumer->name);
	ast_free(consumer);
}

static void free_consumers(void)
{
	struct kafka_consumer *consumer;

	AST_LIST_LOCK(&consumers);
	while ((consumer = AST_LIST_REMOVE_HEAD(&consumers, entry))) {
		consumer_free(consumer);
	}
	AST_LIST_UNLOCK(&consumers);
}

static int parse_general_config(struct ast_config *cfg)
{
	struct ast_variable *var;

	module_enabled = 1;
	init_without_kafka = 1;

	for (var = ast_variable_browse(cfg, "general"); var; var = var->next) {
		if (!strcasecmp(var->name, "enabled")) {
			module_enabled = ast_true(var->value);
		} else if (!strcasecmp(var->name, "init_without_kafka")) {
			init_without_kafka = ast_true(var->value);
		}
	}

	return 0;
}

static int parse_broker_section(struct ast_config *cfg, const char *category)
{
	struct kafka_broker *broker;
	struct ast_variable *var;
	const char *type;

	type = ast_variable_retrieve(cfg, category, "type");
	if (ast_strlen_zero(type) || strcasecmp(type, "broker")) {
		return 0;
	}

	broker = ast_calloc(1, sizeof(*broker));
	if (!broker) {
		return -1;
	}

	ast_mutex_init(&broker->lock);
	broker->name = ast_strdup(category);
	if (!broker->name) {
		broker_free(broker);
		return -1;
	}

	for (var = ast_variable_browse(cfg, category); var; var = var->next) {
		const char *rdkafka_name = var->name;

		if (!strcasecmp(var->name, "type")) {
			continue;
		}

		if (!strcasecmp(var->name, "host")) {
			rdkafka_name = "bootstrap.servers";
		}

		if (!strcasecmp(rdkafka_name, "bootstrap.servers")) {
			ast_free(broker->bootstrap_servers);
			broker->bootstrap_servers = ast_strdup(var->value);
			if (!broker->bootstrap_servers) {
				broker_free(broker);
				return -1;
			}
		}

		if (append_broker_config_var(broker, rdkafka_name, var->value)) {
			broker_free(broker);
			return -1;
		}
	}

	if (ast_strlen_zero(broker->bootstrap_servers)) {
		ast_log(LOG_WARNING, "Kafka broker section [%s] has no bootstrap.servers/host; skipping\n",
			category);
		broker_free(broker);
		return 0;
	}

	{
		char tps_name[128];
		snprintf(tps_name, sizeof(tps_name), "kafka/%s", broker->name);
		broker->tps = ast_taskprocessor_get(tps_name, TPS_REF_DEFAULT);
		if (!broker->tps) {
			ast_log(LOG_ERROR, "Kafka broker [%s]: failed to create taskprocessor\n",
				broker->name);
			broker_free(broker);
			return -1;
		}
	}

	AST_LIST_LOCK(&brokers);
	AST_LIST_INSERT_TAIL(&brokers, broker, entry);
	AST_LIST_UNLOCK(&brokers);

	ast_log(LOG_NOTICE, "Configured Kafka broker [%s] bootstrap.servers=%s\n",
		broker->name, broker->bootstrap_servers);

	return 0;
}

static int set_string_field(char **field, const char *value)
{
	char *copy;

	copy = ast_strdup(value);
	if (!copy) {
		return -1;
	}

	ast_free(*field);
	*field = copy;

	return 0;
}

static int append_eventfilter(struct kafka_producer *producer, const char *name,
	const char *value);

static int parse_producer_section(struct ast_config *cfg, const char *category)
{
	struct kafka_producer *producer;
	struct ast_variable *var;
	const char *type;

	type = ast_variable_retrieve(cfg, category, "type");
	if (ast_strlen_zero(type) || strcasecmp(type, "producer")) {
		return 0;
	}

	producer = ast_calloc(1, sizeof(*producer));
	if (!producer) {
		return -1;
	}

	producer->name = ast_strdup(category);
	if (!producer->name) {
		producer_free(producer);
		return -1;
	}
	AST_LIST_HEAD_INIT_NOLOCK(&producer->eventfilters);

	for (var = ast_variable_browse(cfg, category); var; var = var->next) {
		if (!strcasecmp(var->name, "type")) {
			continue;
		} else if (!strcasecmp(var->name, "broker")) {
			if (set_string_field(&producer->broker, var->value)) {
				producer_free(producer);
				return -1;
			}
		} else if (!strcasecmp(var->name, "topic")) {
			if (set_string_field(&producer->topic, var->value)) {
				producer_free(producer);
				return -1;
			}
		} else if (!strcasecmp(var->name, "key")) {
			if (set_string_field(&producer->key, var->value)) {
				producer_free(producer);
				return -1;
			}
		} else if (!strcasecmp(var->name, "format")) {
			if (!strcasecmp(var->value, "raw")) {
				producer->format_raw = 1;
			} else if (!strcasecmp(var->value, "json")) {
				producer->format_raw = 0;
			} else {
				ast_log(LOG_WARNING, "[%s] unknown format '%s'; using json\n",
					category, var->value);
			}
		} else if (!strcasecmp(var->name, "create_topic")) {
			producer->create_topic = ast_true(var->value);
		} else if (!strcasecmp(var->name, "topic_partitions")) {
			producer->topic_partitions = atoi(var->value);
		} else if (!strcasecmp(var->name, "topic_replication_factor")) {
			producer->topic_replication_factor = atoi(var->value);
		} else if (!strncasecmp(var->name, "eventfilter", strlen("eventfilter"))) {
			if (append_eventfilter(producer, var->name, var->value)) {
				producer_free(producer);
				return -1;
			}
		} else if (!strcasecmp(var->name, "event_filter")) {
			ast_log(LOG_WARNING, "[%s] uses deprecated event_filter; use repeated eventfilter lines like manager.conf\n",
				category);
			if (append_eventfilter(producer, "eventfilter", var->value)) {
				producer_free(producer);
				return -1;
			}
		} else if (!strncasecmp(var->name, "field.", strlen("field."))) {
			struct ast_variable *fvar;
			struct ast_variable *tail;

			fvar = ast_variable_new(var->name + strlen("field."), var->value, "");
			if (!fvar) {
				producer_free(producer);
				return -1;
			}
			if (!producer->fields) {
				producer->fields = fvar;
			} else {
				for (tail = producer->fields; tail->next; tail = tail->next) {
				}
				tail->next = fvar;
			}
		}
	}

	if (ast_strlen_zero(producer->broker)) {
		ast_log(LOG_WARNING, "Kafka producer section [%s] has no broker; skipping\n",
			category);
		producer_free(producer);
		return 0;
	}

	if (ast_strlen_zero(producer->topic)) {
		ast_log(LOG_WARNING, "Kafka producer section [%s] has no topic; skipping\n",
			category);
		producer_free(producer);
		return 0;
	}

	if (producer->create_topic) {
		if (producer->topic_partitions <= 0) {
			producer->topic_partitions = 1;
		}
		if (producer->topic_replication_factor <= 0) {
			producer->topic_replication_factor = 1;
		}
	}

	AST_LIST_LOCK(&producers);
	AST_LIST_INSERT_TAIL(&producers, producer, entry);
	AST_LIST_UNLOCK(&producers);

	ast_log(LOG_NOTICE, "Configured Kafka producer [%s] topic=%s format=%s eventfilters=%u%s\n",
		producer->name, producer->topic, producer->format_raw ? "raw" : "json",
		producer->eventfilter_count,
		producer->create_topic ? " (auto-create topic)" : "");

	return 0;
}

static int parse_consumer_section(struct ast_config *cfg, const char *category)
{
	struct kafka_consumer *consumer;
	struct ast_variable *var;
	const char *type;

	type = ast_variable_retrieve(cfg, category, "type");
	if (ast_strlen_zero(type) || strcasecmp(type, "consumer")) {
		return 0;
	}

	consumer = ast_calloc(1, sizeof(*consumer));
	if (!consumer) {
		return -1;
	}

	consumer->name = ast_strdup(category);
	consumer->thread = AST_PTHREADT_NULL;
	if (!consumer->name) {
		consumer_free(consumer);
		return -1;
	}

	for (var = ast_variable_browse(cfg, category); var; var = var->next) {
		if (!strcasecmp(var->name, "type")) {
			continue;
		} else if (!strcasecmp(var->name, "broker")) {
			if (set_string_field(&consumer->broker_name, var->value)) {
				consumer_free(consumer);
				return -1;
			}
		} else if (!strcasecmp(var->name, "topic")) {
			if (set_string_field(&consumer->topic, var->value)) {
				consumer_free(consumer);
				return -1;
			}
		} else if (!strcasecmp(var->name, "group_id")) {
			if (set_string_field(&consumer->group_id, var->value)) {
				consumer_free(consumer);
				return -1;
			}
		} else if (!strcasecmp(var->name, "reply_topic")) {
			if (set_string_field(&consumer->reply_topic, var->value)) {
				consumer_free(consumer);
				return -1;
			}
		} else if (!strcasecmp(var->name, "create_topic")) {
			consumer->create_topic = ast_true(var->value);
		} else if (!strcasecmp(var->name, "topic_partitions")) {
			consumer->topic_partitions = atoi(var->value);
		} else if (!strcasecmp(var->name, "topic_replication_factor")) {
			consumer->topic_replication_factor = atoi(var->value);
		} else {
			struct ast_variable *cvar;
			struct ast_variable *tail;

			cvar = ast_variable_new(var->name, var->value, "");
			if (!cvar) {
				consumer_free(consumer);
				return -1;
			}
			if (!consumer->config) {
				consumer->config = cvar;
			} else {
				for (tail = consumer->config; tail->next; tail = tail->next) {
				}
				tail->next = cvar;
			}
		}
	}

	if (ast_strlen_zero(consumer->broker_name)) {
		ast_log(LOG_WARNING, "Kafka consumer section [%s] has no broker; skipping\n", category);
		consumer_free(consumer);
		return 0;
	}

	if (ast_strlen_zero(consumer->topic)) {
		ast_log(LOG_WARNING, "Kafka consumer section [%s] has no topic; skipping\n", category);
		consumer_free(consumer);
		return 0;
	}

	if (consumer->create_topic) {
		if (consumer->topic_partitions <= 0) {
			consumer->topic_partitions = 1;
		}
		if (consumer->topic_replication_factor <= 0) {
			consumer->topic_replication_factor = 1;
		}
	}

	AST_LIST_LOCK(&consumers);
	{
		struct kafka_consumer *existing;

		AST_LIST_TRAVERSE(&consumers, existing, entry) {
			if (!strcasecmp(existing->broker_name, consumer->broker_name)) {
				ast_log(LOG_WARNING, "Kafka consumer section [%s] uses broker '%s', "
					"already used by consumer [%s]; skipping\n",
					category, consumer->broker_name, existing->name);
				AST_LIST_UNLOCK(&consumers);
				consumer_free(consumer);
				return 0;
			}
		}
	}
	AST_LIST_INSERT_TAIL(&consumers, consumer, entry);
	AST_LIST_UNLOCK(&consumers);

	ast_log(LOG_NOTICE, "Configured Kafka consumer [%s] topic=%s group_id=%s\n",
		consumer->name, consumer->topic,
		!ast_strlen_zero(consumer->group_id) ? consumer->group_id : "(none)");

	return 0;
}

static int load_config(void)
{
	struct ast_config *cfg;
	struct ast_flags flags = { 0 };
	const char *category = NULL;

	cfg = ast_config_load(CONFIG_FILE, flags);
	if (!cfg || cfg == CONFIG_STATUS_FILEINVALID) {
		ast_log(LOG_ERROR, "Unable to load %s\n", CONFIG_FILE);
		return -1;
	} else if (cfg == CONFIG_STATUS_FILEMISSING) {
		ast_log(LOG_WARNING, "%s is missing; res_kafka will load without brokers\n",
			CONFIG_FILE);
		return 0;
	}

	parse_general_config(cfg);

	while ((category = ast_category_browse(cfg, category))) {
		if (!strcasecmp(category, "general")) {
			continue;
		}

		if (parse_broker_section(cfg, category)) {
			ast_config_destroy(cfg);
			free_consumers();
			free_producers();
			free_brokers();
			return -1;
		}

		if (parse_producer_section(cfg, category)) {
			ast_config_destroy(cfg);
			free_consumers();
			free_producers();
			free_brokers();
			return -1;
		}

		if (parse_consumer_section(cfg, category)) {
			ast_config_destroy(cfg);
			free_consumers();
			free_producers();
			free_brokers();
			return -1;
		}
	}

	ast_config_destroy(cfg);
	return 0;
}

static int wait_for_producer_poll_or_stop(unsigned int seconds)
{
	struct timespec deadline;
	int res = 0;

	clock_gettime(CLOCK_REALTIME, &deadline);
	deadline.tv_sec += seconds;

	ast_mutex_lock(&producer_poll_lock);
	while (!producer_poll_stop && res != ETIMEDOUT) {
		res = ast_cond_timedwait(&producer_poll_cond, &producer_poll_lock, &deadline);
	}
	ast_mutex_unlock(&producer_poll_lock);

	return producer_poll_stop;
}

static int broker_apply_config(rd_kafka_conf_t *conf, const struct kafka_broker *broker,
	char *errstr, size_t errstr_size)
{
	struct ast_variable *var;

	for (var = broker->config; var; var = var->next) {
		if (rd_kafka_conf_set(conf, var->name, var->value, errstr, errstr_size) !=
			RD_KAFKA_CONF_OK) {
			ast_log(LOG_ERROR, "Kafka broker [%s]: invalid config %s=%s: %s\n",
				broker->name, var->name,
				!strcasecmp(var->name, "sasl.password") ? "<hidden>" : var->value,
				errstr);
			return -1;
		}
	}

	return 0;
}

static void kafka_create_topic(rd_kafka_t *rk, const char *topic, int partitions,
	int replication_factor)
{
	rd_kafka_NewTopic_t *new_topic;
	rd_kafka_AdminOptions_t *options;
	rd_kafka_queue_t *queue;
	rd_kafka_event_t *event;
	char errstr[512];

	new_topic = rd_kafka_NewTopic_new(topic, partitions, replication_factor,
		errstr, sizeof(errstr));
	if (!new_topic) {
		ast_log(LOG_ERROR, "Kafka: failed to prepare topic creation for '%s': %s\n",
			topic, errstr);
		return;
	}

	options = rd_kafka_AdminOptions_new(rk, RD_KAFKA_ADMIN_OP_CREATETOPICS);
	queue = rd_kafka_queue_new(rk);
	if (!options || !queue) {
		ast_log(LOG_ERROR, "Kafka: failed to allocate admin request for topic '%s'\n", topic);
		if (queue) {
			rd_kafka_queue_destroy(queue);
		}
		if (options) {
			rd_kafka_AdminOptions_destroy(options);
		}
		rd_kafka_NewTopic_destroy(new_topic);
		return;
	}

	rd_kafka_CreateTopics(rk, &new_topic, 1, options, queue);

	event = rd_kafka_queue_poll(queue, CREATE_TOPIC_TIMEOUT_MS);
	if (event) {
		if (rd_kafka_event_type(event) == RD_KAFKA_EVENT_CREATETOPICS_RESULT) {
			const rd_kafka_CreateTopics_result_t *result;
			const rd_kafka_topic_result_t **results;
			size_t cnt;
			rd_kafka_resp_err_t err;

			result = rd_kafka_event_CreateTopics_result(event);
			results = rd_kafka_CreateTopics_result_topics(result, &cnt);
			err = cnt ? rd_kafka_topic_result_error(results[0]) : RD_KAFKA_RESP_ERR_UNKNOWN;

			if (err == RD_KAFKA_RESP_ERR_NO_ERROR) {
				ast_log(LOG_NOTICE, "Kafka: created topic '%s' (partitions=%d replication=%d)\n",
					topic, partitions, replication_factor);
			} else if (err == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) {
				ast_log(LOG_DEBUG, "Kafka: topic '%s' already exists\n", topic);
			} else {
				ast_log(LOG_ERROR, "Kafka: failed to create topic '%s': %s\n",
					topic, rd_kafka_err2str(err));
			}
		}
		rd_kafka_event_destroy(event);
	} else {
		ast_log(LOG_ERROR, "Kafka: timeout waiting for topic creation result for '%s'\n", topic);
	}

	rd_kafka_queue_destroy(queue);
	rd_kafka_AdminOptions_destroy(options);
	rd_kafka_NewTopic_destroy(new_topic);
}

static struct kafka_broker *find_broker_locked(const char *name);

static int broker_start(struct kafka_broker *broker)
{
	rd_kafka_conf_t *conf;
	rd_kafka_t *rk;
	char errstr[512];

	conf = rd_kafka_conf_new();
	if (!conf) {
		ast_log(LOG_ERROR, "Kafka broker [%s]: failed to allocate config\n", broker->name);
		return -1;
	}

	if (broker_apply_config(conf, broker, errstr, sizeof(errstr))) {
		rd_kafka_conf_destroy(conf);
		return -1;
	}

	/* rd_kafka_new() takes ownership of conf on success and failure. */
	rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	if (!rk) {
		ast_log(LOG_ERROR, "Kafka broker [%s]: failed to create client: %s\n",
			broker->name, errstr);
		return -1;
	}

	ast_mutex_lock(&broker->lock);
	broker->rk = rk;
	ast_mutex_unlock(&broker->lock);

	ast_log(LOG_NOTICE, "Kafka broker [%s]: client created for %s\n",
		broker->name, broker->bootstrap_servers);

	return 0;
}

static int start_brokers(void)
{
	struct kafka_broker *broker;
	int res = 0;

	AST_LIST_LOCK(&brokers);
	AST_LIST_TRAVERSE(&brokers, broker, entry) {
		ast_log(LOG_NOTICE, "Kafka broker [%s]: creating client for %s\n",
			broker->name, broker->bootstrap_servers);

		if (broker_start(broker)) {
			res = -1;
		}
	}
	AST_LIST_UNLOCK(&brokers);

	return res;
}

static int start_producers(void)
{
	struct kafka_producer *producer;

	AST_LIST_LOCK(&producers);
	AST_LIST_TRAVERSE(&producers, producer, entry) {
		struct kafka_broker *broker;
		rd_kafka_t *rk;

		if (!producer->create_topic) {
			continue;
		}

		AST_LIST_LOCK(&brokers);
		broker = find_broker_locked(producer->broker);
		rk = broker ? broker->rk : NULL;
		AST_LIST_UNLOCK(&brokers);

		if (!rk) {
			ast_log(LOG_WARNING, "Kafka producer [%s]: broker '%s' unavailable, "
				"skipping topic creation for '%s'\n",
				producer->name, producer->broker, producer->topic);
			continue;
		}

		AST_LIST_UNLOCK(&producers);
		kafka_create_topic(rk, producer->topic,
			producer->topic_partitions, producer->topic_replication_factor);
		AST_LIST_LOCK(&producers);
	}
	AST_LIST_UNLOCK(&producers);

	return 0;
}

static struct kafka_broker *find_broker_locked(const char *name)
{
	struct kafka_broker *broker;

	AST_LIST_TRAVERSE(&brokers, broker, entry) {
		if (!strcasecmp(broker->name, name)) {
			return broker;
		}
	}
	return NULL;
}

static char *extract_header_value(const char *body, const char *header);

static int should_log_counter(int count)
{
	return count <= 5 || !(count & (count - 1));
}

static void note_no_producer_drop(const char *broker_name, const char *topic)
{
	int count = ast_atomic_fetchadd_int(&messages_lost_no_producer, +1) + 1;

	if (should_log_counter(count)) {
		ast_log(LOG_WARNING, "Kafka broker [%s]: producer unavailable, dropped message to %s (total=%d)\n",
			broker_name, topic, count);
	}
}

static void note_produce_failure(const char *broker_name, const char *topic,
	rd_kafka_resp_err_t err)
{
	int count = ast_atomic_fetchadd_int(&messages_lost_produce_failed, +1) + 1;

	if (should_log_counter(count)) {
		ast_log(LOG_WARNING, "Kafka broker [%s]: failed to enqueue to %s: %s (total=%d)\n",
			broker_name, topic, rd_kafka_err2str(err), count);
	}
}

static int copy_criteria_value(const char *criteria, const char *name, char *buf, size_t size)
{
	char search[64];
	const char *start;
	const char *end;
	size_t len;

	snprintf(search, sizeof(search), "%s(", name);
	start = strcasestr(criteria, search);
	if (!start) {
		return 0;
	}

	start += strlen(search);
	end = strchr(start, ')');
	if (!end) {
		return 0;
	}

	len = end - start;
	if (len >= size) {
		len = size - 1;
	}
	memcpy(buf, start, len);
	buf[len] = '\0';

	return 1;
}

static int compile_eventfilter_regex(struct kafka_eventfilter *filter, const char *expression)
{
	if (regcomp(&filter->regex, expression, REG_EXTENDED | REG_NOSUB)) {
		return -1;
	}

	filter->has_regex = 1;
	return 0;
}

static int init_eventfilter(struct kafka_eventfilter *filter)
{
	char criteria[256];
	const char *criteria_start;
	const char *criteria_end;
	size_t criteria_len;

	filter->match_expression = filter->value;

	if (filter->value[0] == '!' && !strcasecmp(filter->name, "eventfilter")) {
		filter->is_exclude = 1;
		filter->match_expression = filter->value + 1;
	}

	if (!strncasecmp(filter->name, "eventfilter(", strlen("eventfilter("))) {
		filter->is_advanced = 1;

		criteria_start = filter->name + strlen("eventfilter(");
		criteria_end = strrchr(criteria_start, ')');
		if (!criteria_end) {
			return -1;
		}

		criteria_len = criteria_end - criteria_start;
		if (criteria_len >= sizeof(criteria)) {
			criteria_len = sizeof(criteria) - 1;
		}
		memcpy(criteria, criteria_start, criteria_len);
		criteria[criteria_len] = '\0';

		if (copy_criteria_value(criteria, "action", filter->action, sizeof(filter->action))
			&& !strcasecmp(filter->action, "exclude")) {
			filter->is_exclude = 1;
		}
		copy_criteria_value(criteria, "name", filter->event_name, sizeof(filter->event_name));
		copy_criteria_value(criteria, "header", filter->header, sizeof(filter->header));
		copy_criteria_value(criteria, "method", filter->method, sizeof(filter->method));
	} else if (strcasecmp(filter->name, "eventfilter")) {
		return -1;
	}

	if ((!filter->is_advanced || !strcasecmp(filter->method, "regex"))
		&& compile_eventfilter_regex(filter, filter->match_expression)) {
		return -1;
	}

	return 0;
}

static int append_eventfilter(struct kafka_producer *producer, const char *name,
	const char *value)
{
	struct kafka_eventfilter *filter;

	filter = ast_calloc(1, sizeof(*filter));
	if (!filter) {
		return -1;
	}

	filter->name = ast_strdup(name);
	filter->value = ast_strdup(value);
	if (!filter->name || !filter->value || init_eventfilter(filter)) {
		if (filter->has_regex) {
			regfree(&filter->regex);
		}
		ast_free(filter->name);
		ast_free(filter->value);
		ast_free(filter);
		return -1;
	}

	AST_LIST_INSERT_TAIL(&producer->eventfilters, filter, entry);
	producer->eventfilter_count++;

	return 0;
}

static int match_value(const struct kafka_eventfilter *filter, const char *data)
{
	const char *method = filter->method;
	const char *expression = filter->match_expression;

	if (!strcasecmp(method, "none") || (ast_strlen_zero(method) && ast_strlen_zero(expression))) {
		return 1;
	}

	if (ast_strlen_zero(expression)) {
		return 0;
	}

	if (!strcasecmp(method, "exact")) {
		return !strcmp(data, expression);
	} else if (!strcasecmp(method, "starts_with")) {
		return !strncmp(data, expression, strlen(expression));
	} else if (!strcasecmp(method, "ends_with")) {
		size_t data_len = strlen(data);
		size_t expression_len = strlen(expression);

		return data_len >= expression_len
			&& !strcmp(data + data_len - expression_len, expression);
	} else if (!strcasecmp(method, "contains")) {
		return strstr(data, expression) != NULL;
	} else if (!strcasecmp(method, "regex")) {
		return filter->has_regex && !regexec(&filter->regex, data, 0, NULL, 0);
	} else if (ast_strlen_zero(method)) {
		return strstr(data, expression) != NULL;
	}

	return 0;
}

static int advanced_eventfilter_matches(const struct kafka_eventfilter *filter,
	const char *event, const char *body, int *is_exclude)
{
	char *header_value = NULL;
	const char *data;
	int matched;

	*is_exclude = filter->is_exclude;

	if (!ast_strlen_zero(filter->event_name) && strcasecmp(filter->event_name, event)) {
		return 0;
	}

	if (!ast_strlen_zero(filter->header)) {
		header_value = extract_header_value(body, filter->header);
		if (!header_value) {
			return 0;
		}
		data = header_value;
	} else {
		data = body;
	}

	matched = match_value(filter, data);
	ast_free(header_value);

	return matched;
}

static int legacy_eventfilter_matches(const struct kafka_eventfilter *filter,
	const char *body, int *is_exclude)
{
	*is_exclude = filter->is_exclude;

	return filter->has_regex && !regexec(&filter->regex, body, 0, NULL, 0);
}

/* Returns 1 if the event passes the producer's filters (or there are none). */
static int eventfilter_matches(const struct kafka_producer *producer,
	const char *event, const char *body)
{
	struct kafka_eventfilter *filter;
	int has_include = 0;
	int included = 0;

	if (!producer->eventfilter_count) {
		return 1;
	}

	AST_LIST_TRAVERSE(&producer->eventfilters, filter, entry) {
		int is_exclude = 0;
		int matched;

		if (filter->is_advanced) {
			matched = advanced_eventfilter_matches(filter, event, body, &is_exclude);
		} else {
			matched = legacy_eventfilter_matches(filter, body, &is_exclude);
		}

		if (!is_exclude) {
			has_include = 1;
		}

		if (matched && is_exclude) {
			return 0;
		} else if (matched) {
			included = 1;
		}
	}

	return has_include ? included : 1;
}

/* Extracts the value of a "Header: value\r\n" line from an AMI event body.
 * Parses line by line so "Linkedid" does not match "DestLinkedid". */
static char *extract_header_value(const char *body, const char *header)
{
	size_t header_len = strlen(header);
	const char *line = body;

	while (line && *line) {
		const char *eol = strstr(line, "\r\n");
		const char *colon = memchr(line, ':', eol ? (size_t)(eol - line) : strlen(line));

		if (colon && (size_t)(colon - line) == header_len
			&& !strncasecmp(line, header, header_len)) {
			const char *value = colon + 1;
			const char *end = eol ? eol : value + strlen(value);

			while (value < end && *value == ' ') {
				value++;
			}
			return ast_strndup(value, end - value);
		}

		line = eol ? eol + 2 : NULL;
	}

	return NULL;
}

/* Appends str[0..len) to buf with JSON string escaping. */
static void json_append_escaped_n(struct ast_str **buf, const char *str, size_t len)
{
	const char *start = str;
	const char *p = str;
	const char *end = str + len;

	while (p < end) {
		unsigned char c = (unsigned char)*p;

		if (c == '"' || c == '\\' || c < 0x20) {
			if (p > start) {
				ast_str_append(buf, 0, "%.*s", (int)(p - start), start);
			}
			switch (c) {
			case '"':  ast_str_append(buf, 0, "\\\""); break;
			case '\\': ast_str_append(buf, 0, "\\\\"); break;
			case '\n': ast_str_append(buf, 0, "\\n");  break;
			case '\r': ast_str_append(buf, 0, "\\r");  break;
			case '\t': ast_str_append(buf, 0, "\\t");  break;
			default:   ast_str_append(buf, 0, "\\u%04x", c); break;
			}
			start = p + 1;
		}
		p++;
	}

	if (p > start) {
		ast_str_append(buf, 0, "%.*s", (int)(p - start), start);
	}
}

/* Converts an AMI event body ("Key: Value\r\n...") to a JSON object string,
 * appending any extra fields from the producer's field.* config. */
static char *ami_body_to_json(const char *body, const struct ast_variable *fields)
{
	struct ast_str *buf;
	const char *line;
	const struct ast_variable *fvar;
	int first = 1;

	buf = ast_str_create(512);
	if (!buf) {
		return NULL;
	}

	ast_str_append(&buf, 0, "{");

	for (line = body; line && *line; ) {
		const char *eol = strstr(line, "\r\n");
		size_t line_len = eol ? (size_t)(eol - line) : strlen(line);
		const char *colon = memchr(line, ':', line_len);

		if (colon) {
			const char *key_start = line;
			const char *key_end = colon;
			const char *val_start = colon + 1;
			const char *val_end = line + line_len;

			while (val_start < val_end && *val_start == ' ') {
				val_start++;
			}

			if (key_end > key_start) {
				if (!first) {
					ast_str_append(&buf, 0, ",");
				}
				first = 0;
				ast_str_append(&buf, 0, "\"");
				json_append_escaped_n(&buf, key_start, key_end - key_start);
				ast_str_append(&buf, 0, "\":\"");
				json_append_escaped_n(&buf, val_start, val_end - val_start);
				ast_str_append(&buf, 0, "\"");
			}
		}

		line = eol ? eol + 2 : NULL;
	}

	for (fvar = fields; fvar; fvar = fvar->next) {
		if (!first) {
			ast_str_append(&buf, 0, ",");
		}
		first = 0;
		ast_str_append(&buf, 0, "\"");
		json_append_escaped_n(&buf, fvar->name, strlen(fvar->name));
		ast_str_append(&buf, 0, "\":\"");
		json_append_escaped_n(&buf, fvar->value, strlen(fvar->value));
		ast_str_append(&buf, 0, "\"");
	}

	ast_str_append(&buf, 0, "}");

	{
		char *result = ast_strdup(ast_str_buffer(buf));
		ast_free(buf);
		return result;
	}
}

/* Returns the raw AMI event body with the producer's field.* lines appended.
 * The body ends with a blank "\r\n" line that terminates the event; the extra
 * fields are inserted before it so they stay part of the same event. */
static char *ami_body_to_raw(const char *body, const struct ast_variable *fields)
{
	struct ast_str *buf;
	const struct ast_variable *fvar;
	const char *p;
	size_t len;

	if (!fields) {
		return ast_strdup(body);
	}

	buf = ast_str_create(512);
	if (!buf) {
		return NULL;
	}

	ast_str_set(&buf, 0, "%s", body);
	len = ast_str_strlen(buf);
	p = ast_str_buffer(buf);

	if (len >= 4 && !strcmp(p + len - 4, "\r\n\r\n")) {
		/* Drop the terminating blank line; re-added after the fields below. */
		ast_str_truncate(buf, len - 2);
	} else if (len && !(len >= 2 && !strcmp(p + len - 2, "\r\n"))) {
		/* Body does not end on a header line; start a fresh one. */
		ast_str_append(&buf, 0, "\r\n");
	}

	for (fvar = fields; fvar; fvar = fvar->next) {
		ast_str_append(&buf, 0, "%s: %s\r\n", fvar->name, fvar->value);
	}

	ast_str_append(&buf, 0, "\r\n");

	{
		char *result = ast_strdup(ast_str_buffer(buf));
		ast_free(buf);
		return result;
	}
}

struct kafka_send_task {
	struct kafka_broker *broker;
	char *topic;
	char *key;
	size_t key_len;
	char *body;
	size_t body_len;
};

static int send_task_execute(void *data)
{
	struct kafka_send_task *task = data;
	rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;

	ast_mutex_lock(&task->broker->lock);
	if (task->broker->rk) {
		if (task->key) {
			err = rd_kafka_producev(
				task->broker->rk,
				RD_KAFKA_V_TOPIC(task->topic),
				RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
				RD_KAFKA_V_VALUE(task->body, task->body_len),
				RD_KAFKA_V_KEY(task->key, task->key_len),
				RD_KAFKA_V_END
			);
		} else {
			err = rd_kafka_producev(
				task->broker->rk,
				RD_KAFKA_V_TOPIC(task->topic),
				RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
				RD_KAFKA_V_VALUE(task->body, task->body_len),
				RD_KAFKA_V_END
			);
		}
		rd_kafka_poll(task->broker->rk, 0);
	} else {
		note_no_producer_drop(task->broker->name, task->topic);
	}
	ast_mutex_unlock(&task->broker->lock);

	if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
		note_produce_failure(task->broker->name, task->topic, err);
	}

	ast_free(task->key);
	ast_free(task->body);
	ast_free(task->topic);
	ast_free(task);

	return 0;
}

static int ami_event_hook(int category, const char *event, char *body)
{
	struct kafka_producer *producer;
	char *override_key;
	char *override_topic;

	override_key = extract_header_value(body, KAFKA_OVERRIDE_KEY_HEADER);
	override_topic = extract_header_value(body, KAFKA_OVERRIDE_TOPIC_HEADER);

	if (override_topic && ast_strlen_zero(override_topic)) {
		ast_free(override_topic);
		override_topic = NULL;
	}

	AST_LIST_TRAVERSE(&producers, producer, entry) {
		struct kafka_broker *broker;
		struct ast_taskprocessor *tps;
		struct kafka_send_task *task;
		char *key = NULL;

		if (!eventfilter_matches(producer, event, body)) {
			continue;
		}

		AST_LIST_LOCK(&brokers);
		broker = find_broker_locked(producer->broker);
		tps = broker ? broker->tps : NULL;
		AST_LIST_UNLOCK(&brokers);

		if (!tps) {
			continue;
		}

		if (override_key) {
			key = ast_strdup(override_key);
			if (!key) {
				ast_log(LOG_WARNING, "Kafka producer [%s]: dropped event '%s': allocation failure\n",
					producer->name, event);
				continue;
			}
		} else if (!ast_strlen_zero(producer->key)) {
			key = extract_header_value(body, producer->key);
		}

		task = ast_malloc(sizeof(*task));
		if (!task) {
			ast_free(key);
			continue;
		}

		task->broker   = broker;
		task->topic    = ast_strdup(override_topic ? override_topic : producer->topic);
		task->key      = key;
		task->key_len  = key ? strlen(key) : 0;
		task->body     = producer->format_raw
			? ami_body_to_raw(body, producer->fields)
			: ami_body_to_json(body, producer->fields);
		task->body_len = task->body ? strlen(task->body) : 0;

		if (!task->topic || !task->body
			|| ast_taskprocessor_push(tps, send_task_execute, task)) {
			ast_log(LOG_WARNING, "Kafka producer [%s]: dropped event '%s': %s\n",
				producer->name, event,
				(!task->topic || !task->body) ? "allocation failure" : "taskprocessor queue full");
			ast_free(task->topic);
			ast_free(task->body);
			ast_free(task->key);
			ast_free(task);
		}
	}

	ast_free(override_key);
	ast_free(override_topic);

	return 0;
}

static struct manager_custom_hook kafka_ami_hook = {
	.file = __FILE__,
	.helper = ami_event_hook,
};

static void *producer_poll_thread_main(void *unused)
{
	struct kafka_broker *broker;

	while (!producer_poll_stop) {
		AST_LIST_LOCK(&brokers);
		AST_LIST_TRAVERSE(&brokers, broker, entry) {
			ast_mutex_lock(&broker->lock);
			if (broker->rk) {
				int outq = rd_kafka_outq_len(broker->rk);
				if (outq > 0) {
					ast_log(LOG_DEBUG, "Kafka broker [%s]: %d message(s) in output queue\n",
						broker->name, outq);
				}
				rd_kafka_poll(broker->rk, 0);
			}
			ast_mutex_unlock(&broker->lock);
		}
		AST_LIST_UNLOCK(&brokers);

		if (wait_for_producer_poll_or_stop(PRODUCER_POLL_INTERVAL_SEC)) {
			break;
		}
	}

	return NULL;
}

static int start_producer_poll_thread(void)
{
	producer_poll_stop = 0;

	if (ast_pthread_create_background(&producer_poll_thread, NULL, producer_poll_thread_main, NULL)) {
		ast_log(LOG_ERROR, "Failed to start Kafka producer poll thread\n");
		producer_poll_thread = AST_PTHREADT_NULL;
		return -1;
	}

	return 0;
}

static void stop_producer_poll_thread(void)
{
	if (producer_poll_thread == AST_PTHREADT_NULL) {
		return;
	}

	ast_mutex_lock(&producer_poll_lock);
	producer_poll_stop = 1;
	ast_cond_signal(&producer_poll_cond);
	ast_mutex_unlock(&producer_poll_lock);

	pthread_join(producer_poll_thread, NULL);
	producer_poll_thread = AST_PTHREADT_NULL;
}

/* Thread-local: set by consumer_thread_main around ast_hook_send_action. */
static __thread struct {
	struct kafka_consumer *consumer;
	char *reply_topic;  /*!< from message field, overrides consumer->reply_topic */
	struct ast_str *response_buf;
} consumer_reply_ctx;

static int publish_consumer_action_response(struct kafka_consumer *consumer,
	const char *reply_topic, const char *body)
{
	struct kafka_broker *broker;
	struct kafka_send_task *task;
	char *json;

	json = ami_body_to_json(body, NULL);
	if (!json) {
		return -1;
	}

	AST_LIST_LOCK(&brokers);
	broker = find_broker_locked(consumer->broker_name);
	AST_LIST_UNLOCK(&brokers);

	if (!broker) {
		ast_free(json);
		return -1;
	}

	task = ast_malloc(sizeof(*task));
	if (!task) {
		ast_free(json);
		return -1;
	}

	task->broker   = broker;
	task->topic    = ast_strdup(reply_topic);
	task->key      = NULL;
	task->key_len  = 0;
	task->body     = json;
	task->body_len = strlen(json);

	if (!task->topic || ast_taskprocessor_push(broker->tps, send_task_execute, task)) {
		ast_free(task->topic);
		ast_free(task->body);
		ast_free(task);
		return -1;
	}

	return 0;
}

static void compact_consumer_response_buffer(size_t used)
{
	size_t remaining = ast_str_strlen(consumer_reply_ctx.response_buf) - used;
	char *buffer = ast_str_buffer(consumer_reply_ctx.response_buf);

	if (remaining) {
		memmove(buffer, buffer + used, remaining);
	}
	ast_str_truncate(consumer_reply_ctx.response_buf, remaining);
}

static int consumer_action_response_handler(int category, const char *event, char *body)
{
	struct kafka_consumer *consumer = consumer_reply_ctx.consumer;
	const char *reply_topic;
	char *end;

	if (!consumer || category != EVENT_FLAG_HOOKRESPONSE || strcmp(event, "HookResponse")) {
		return 0;
	}

	reply_topic = !ast_strlen_zero(consumer_reply_ctx.reply_topic)
		? consumer_reply_ctx.reply_topic
		: consumer->reply_topic;

	if (ast_strlen_zero(reply_topic)) {
		return 0;
	}

	if (!consumer_reply_ctx.response_buf) {
		consumer_reply_ctx.response_buf = ast_str_create(512);
		if (!consumer_reply_ctx.response_buf) {
			return 0;
		}
	}

	if (ast_str_append(&consumer_reply_ctx.response_buf, 0, "%s", body) == AST_DYNSTR_BUILD_FAILED) {
		ast_str_reset(consumer_reply_ctx.response_buf);
		return 0;
	}

	while ((end = strstr(ast_str_buffer(consumer_reply_ctx.response_buf), "\r\n\r\n"))) {
		size_t frame_len = end - ast_str_buffer(consumer_reply_ctx.response_buf) + 4;
		char *frame = ast_strndup(ast_str_buffer(consumer_reply_ctx.response_buf), frame_len);

		if (frame) {
			publish_consumer_action_response(consumer, reply_topic, frame);
			ast_free(frame);
		}

		compact_consumer_response_buffer(frame_len);
	}

	return 0;
}

static struct manager_custom_hook kafka_consumer_hook = {
	.file = __FILE__,
	.helper = consumer_action_response_handler,
};

/*
 * Converts a JSON object to AMI action text ("Key: Value\r\n...\r\n").
 * The field "reply_topic" is stripped from the AMI output and returned via
 * reply_topic_out (caller must ast_free it). Pass NULL to ignore.
 */
static int ami_header_name_is_safe(const char *key)
{
	const unsigned char *pos;

	if (ast_strlen_zero(key)) {
		return 0;
	}

	for (pos = (const unsigned char *) key; *pos; ++pos) {
		if (!isalnum(*pos) && *pos != '-' && *pos != '_') {
			return 0;
		}
	}

	return 1;
}

static int ami_header_value_is_safe(const char *value)
{
	return value && !strchr(value, '\r') && !strchr(value, '\n');
}

static char *json_to_ami(const char *json_str, size_t len, char **reply_topic_out)
{
	struct ast_json_error error;
	struct ast_json *obj;
	struct ast_json_iter *iter;
	struct ast_str *buf;
	char *result;
	int action_seen = 0;

	if (reply_topic_out) {
		*reply_topic_out = NULL;
	}

	obj = ast_json_load_buf(json_str, len, &error);
	if (!obj) {
		ast_log(LOG_WARNING, "Kafka consumer: failed to parse JSON: %s\n", error.text);
		return NULL;
	}

	if (ast_json_typeof(obj) != AST_JSON_OBJECT) {
		ast_log(LOG_WARNING, "Kafka consumer: expected JSON object\n");
		ast_json_unref(obj);
		return NULL;
	}

	buf = ast_str_create(256);
	if (!buf) {
		ast_json_unref(obj);
		return NULL;
	}

	for (iter = ast_json_object_iter(obj); iter;
	     iter = ast_json_object_iter_next(obj, iter)) {
		const char *key = ast_json_object_iter_key(iter);
		struct ast_json *val = ast_json_object_iter_value(iter);

		if (reply_topic_out && !strcasecmp(key, "reply_topic")
		    && ast_json_typeof(val) == AST_JSON_STRING) {
			const char *reply_topic = ast_json_string_get(val);

			if (!ami_header_value_is_safe(reply_topic)) {
				ast_log(LOG_WARNING, "Kafka consumer: unsafe reply_topic value\n");
				goto fail;
			}
			*reply_topic_out = ast_strdup(reply_topic);
			continue;
		}

		if (!ami_header_name_is_safe(key)) {
			ast_log(LOG_WARNING, "Kafka consumer: unsafe AMI header name '%s'\n", key);
			goto fail;
		}

		if (!strcasecmp(key, "Action")) {
			if (ast_json_typeof(val) != AST_JSON_STRING
				|| ast_strlen_zero(ast_json_string_get(val))) {
				ast_log(LOG_WARNING, "Kafka consumer: AMI Action must be a non-empty string\n");
				goto fail;
			}
			action_seen = 1;
		}

		switch (ast_json_typeof(val)) {
		case AST_JSON_STRING:
			if (!ami_header_value_is_safe(ast_json_string_get(val))) {
				ast_log(LOG_WARNING, "Kafka consumer: unsafe value for AMI header '%s'\n", key);
				goto fail;
			}
			ast_str_append(&buf, 0, "%s: %s\r\n", key, ast_json_string_get(val));
			break;
		case AST_JSON_INTEGER:
			ast_str_append(&buf, 0, "%s: %lld\r\n", key,
				(long long)ast_json_integer_get(val));
			break;
		case AST_JSON_TRUE:
			ast_str_append(&buf, 0, "%s: true\r\n", key);
			break;
		case AST_JSON_FALSE:
			ast_str_append(&buf, 0, "%s: false\r\n", key);
			break;
		default:
			/* Skip objects, arrays, null — AMI headers are flat. */
			break;
		}
	}

	if (!action_seen) {
		ast_log(LOG_WARNING, "Kafka consumer: AMI action message has no Action field\n");
		goto fail;
	}

	ast_str_append(&buf, 0, "\r\n");

	result = ast_strdup(ast_str_buffer(buf));
	ast_free(buf);
	ast_json_unref(obj);
	return result;

fail:
	if (reply_topic_out) {
		ast_free(*reply_topic_out);
		*reply_topic_out = NULL;
	}
	ast_free(buf);
	ast_json_unref(obj);
	return NULL;
}

static void *consumer_thread_main(void *data)
{
	struct kafka_consumer *consumer = data;
	int err_count = 0;
	time_t last_topic_create = 0;

	if (consumer->create_topic) {
		kafka_create_topic(consumer->rk, consumer->topic,
			consumer->topic_partitions, consumer->topic_replication_factor);
		last_topic_create = time(NULL);
	}

	while (ast_atomic_fetchadd_int(&consumer->running, 0)) {
		rd_kafka_message_t *msg = rd_kafka_consumer_poll(consumer->rk, 500);
		if (!msg) {
			continue;
		}

		ast_log(LOG_DEBUG, "Kafka consumer [%s]: received message partition=%d offset=%lld len=%zd\n",
			consumer->name, msg->partition, (long long)msg->offset, msg->len);

		if (msg->err) {
			if (msg->err == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART && consumer->create_topic) {
				time_t now = time(NULL);
				if (now - last_topic_create >= 30) {
					ast_log(LOG_NOTICE, "Kafka consumer [%s]: topic '%s' missing after reconnect, recreating\n",
						consumer->name, consumer->topic);
					kafka_create_topic(consumer->rk, consumer->topic,
						consumer->topic_partitions, consumer->topic_replication_factor);
					last_topic_create = now;
				}
			} else if (msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
				if (should_log_counter(++err_count)) {
					ast_log(LOG_WARNING, "Kafka consumer [%s]: %s\n",
						consumer->name, rd_kafka_message_errstr(msg));
				}
			}
		} else if (msg->payload && msg->len > 0) {

			char *msg_reply_topic = NULL;
			char *ami_msg = json_to_ami((const char *)msg->payload,
				(size_t)msg->len, &msg_reply_topic);
			if (ami_msg) {
				consumer_reply_ctx.consumer    = consumer;
				consumer_reply_ctx.reply_topic = msg_reply_topic;
				ast_hook_send_action(&kafka_consumer_hook, ami_msg);
				if (consumer_reply_ctx.response_buf
					&& ast_str_strlen(consumer_reply_ctx.response_buf)) {
					ast_log(LOG_WARNING, "Kafka consumer [%s]: discarded incomplete AMI response fragment\n",
						consumer->name);
					ast_str_reset(consumer_reply_ctx.response_buf);
				}
				consumer_reply_ctx.consumer    = NULL;
				consumer_reply_ctx.reply_topic = NULL;
				ast_free(ami_msg);
			}
			ast_free(msg_reply_topic);
			if (!ami_msg) {
				ast_log(LOG_WARNING, "Kafka consumer [%s]: failed to convert message\n",
					consumer->name);
			}
		}

		rd_kafka_message_destroy(msg);
	}

	if (consumer_reply_ctx.response_buf) {
		ast_free(consumer_reply_ctx.response_buf);
		consumer_reply_ctx.response_buf = NULL;
	}

	rd_kafka_consumer_close(consumer->rk);

	return NULL;
}

static int consumer_start(struct kafka_consumer *consumer)
{
	struct kafka_broker *broker;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_partition_list_t *topics;
	rd_kafka_resp_err_t err;
	struct ast_variable *var;
	char errstr[512];

	AST_LIST_LOCK(&brokers);
	broker = find_broker_locked(consumer->broker_name);
	AST_LIST_UNLOCK(&brokers);

	if (!broker) {
		ast_log(LOG_ERROR, "Kafka consumer [%s]: broker '%s' not found\n",
			consumer->name, consumer->broker_name);
		return -1;
	}

	conf = rd_kafka_conf_new();
	if (!conf) {
		ast_log(LOG_ERROR, "Kafka consumer [%s]: failed to allocate config\n", consumer->name);
		return -1;
	}

	if (broker_apply_config(conf, broker, errstr, sizeof(errstr))) {
		rd_kafka_conf_destroy(conf);
		return -1;
	}

	if (!ast_strlen_zero(consumer->group_id) &&
	    rd_kafka_conf_set(conf, "group.id", consumer->group_id,
	                      errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		ast_log(LOG_ERROR, "Kafka consumer [%s]: failed to set group.id: %s\n",
			consumer->name, errstr);
		rd_kafka_conf_destroy(conf);
		return -1;
	}

	for (var = consumer->config; var; var = var->next) {
		if (rd_kafka_conf_set(conf, var->name, var->value,
		                      errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
			ast_log(LOG_ERROR, "Kafka consumer [%s]: invalid config %s=%s: %s\n",
				consumer->name, var->name, var->value, errstr);
			rd_kafka_conf_destroy(conf);
			return -1;
		}
	}

	consumer->rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
	if (!consumer->rk) {
		ast_log(LOG_ERROR, "Kafka consumer [%s]: failed to create consumer: %s\n",
			consumer->name, errstr);
		return -1;
	}

	rd_kafka_poll_set_consumer(consumer->rk);

	topics = rd_kafka_topic_partition_list_new(1);
	rd_kafka_topic_partition_list_add(topics, consumer->topic, RD_KAFKA_PARTITION_UA);
	err = rd_kafka_subscribe(consumer->rk, topics);
	rd_kafka_topic_partition_list_destroy(topics);

	if (err) {
		ast_log(LOG_ERROR, "Kafka consumer [%s]: failed to subscribe to '%s': %s\n",
			consumer->name, consumer->topic, rd_kafka_err2str(err));
		rd_kafka_consumer_close(consumer->rk);
		rd_kafka_destroy(consumer->rk);
		consumer->rk = NULL;
		return -1;
	}

	ast_log(LOG_NOTICE, "Kafka consumer [%s]: subscribed to topic '%s'\n",
		consumer->name, consumer->topic);

	ast_atomic_fetchadd_int(&consumer->running, +1);
	if (ast_pthread_create_background(&consumer->thread, NULL, consumer_thread_main, consumer)) {
		ast_log(LOG_ERROR, "Kafka consumer [%s]: failed to start thread\n", consumer->name);
		ast_atomic_fetchadd_int(&consumer->running, -1);
		rd_kafka_consumer_close(consumer->rk);
		rd_kafka_destroy(consumer->rk);
		consumer->rk = NULL;
		return -1;
	}

	return 0;
}

static int start_consumers(void)
{
	struct kafka_consumer *consumer;
	int failures = 0;

	AST_LIST_TRAVERSE(&consumers, consumer, entry) {
		if (consumer_start(consumer)) {
			failures++;
		}
	}

	return failures;
}

static void consumer_stop(struct kafka_consumer *consumer)
{
	ast_atomic_fetchadd_int(&consumer->running, -1);
}

static void stop_consumers(void)
{
	struct kafka_consumer *consumer;

	AST_LIST_LOCK(&consumers);
	AST_LIST_TRAVERSE(&consumers, consumer, entry) {
		consumer_stop(consumer);
	}
	AST_LIST_UNLOCK(&consumers);

	AST_LIST_TRAVERSE(&consumers, consumer, entry) {
		if (consumer->thread != AST_PTHREADT_NULL) {
			pthread_join(consumer->thread, NULL);
			consumer->thread = AST_PTHREADT_NULL;
		}
	}
}

static int load_module(void)
{
	ast_mutex_init(&producer_poll_lock);
	ast_cond_init(&producer_poll_cond, NULL);
	hooks_registered = 0;

	if (load_config()) {
		if (!init_without_kafka) {
			ast_cond_destroy(&producer_poll_cond);
			ast_mutex_destroy(&producer_poll_lock);
			return AST_MODULE_LOAD_DECLINE;
		}

		ast_log(LOG_ERROR, "Kafka config failed, but init_without_kafka=yes; continuing\n");
	}

	if (!module_enabled) {
		ast_log(LOG_NOTICE, "res_kafka is disabled by config\n");
		free_consumers();
		free_producers();
		free_brokers();
		return AST_MODULE_LOAD_SUCCESS;
	}

	if (start_brokers()) {
		if (!init_without_kafka) {
			free_consumers();
			free_producers();
			free_brokers();
			ast_cond_destroy(&producer_poll_cond);
			ast_mutex_destroy(&producer_poll_lock);
			return AST_MODULE_LOAD_DECLINE;
		}

		ast_log(LOG_ERROR, "One or more Kafka brokers failed to start, but init_without_kafka=yes; continuing\n");
	}

	if (start_producers()) {
		if (!init_without_kafka) {
			free_consumers();
			free_producers();
			free_brokers();
			ast_cond_destroy(&producer_poll_cond);
			ast_mutex_destroy(&producer_poll_lock);
			return AST_MODULE_LOAD_DECLINE;
		}

		ast_log(LOG_ERROR, "One or more Kafka producers failed to start, but init_without_kafka=yes; continuing\n");
	}

	if (start_producer_poll_thread()) {
		if (!init_without_kafka) {
			free_consumers();
			free_producers();
			free_brokers();
			ast_cond_destroy(&producer_poll_cond);
			ast_mutex_destroy(&producer_poll_lock);
			return AST_MODULE_LOAD_DECLINE;
		}

		ast_log(LOG_ERROR, "Kafka producer poll thread failed, but init_without_kafka=yes; continuing\n");
	}

	ast_manager_register_hook(&kafka_ami_hook);
	ast_manager_register_hook(&kafka_consumer_hook);
	hooks_registered = 1;

	if (start_consumers()) {
		if (!init_without_kafka) {
			ast_manager_unregister_hook(&kafka_consumer_hook);
			ast_manager_unregister_hook(&kafka_ami_hook);
			hooks_registered = 0;
			stop_consumers();
			stop_producer_poll_thread();
			free_consumers();
			free_producers();
			free_brokers();
			ast_cond_destroy(&producer_poll_cond);
			ast_mutex_destroy(&producer_poll_lock);
			return AST_MODULE_LOAD_DECLINE;
		}

		ast_log(LOG_ERROR, "One or more Kafka consumers failed to start, but init_without_kafka=yes; continuing\n");
	}

	return AST_MODULE_LOAD_SUCCESS;
}

static int unload_module(void)
{
	if (hooks_registered) {
		ast_manager_unregister_hook(&kafka_consumer_hook);
		ast_manager_unregister_hook(&kafka_ami_hook);
		hooks_registered = 0;
	}
	stop_consumers();
	stop_producer_poll_thread();
	free_consumers();
	free_producers();
	free_brokers();
	ast_cond_destroy(&producer_poll_cond);
	ast_mutex_destroy(&producer_poll_lock);

	return 0;
}

AST_MODULE_INFO_STANDARD(ASTERISK_GPL_KEY, "Kafka integration");
