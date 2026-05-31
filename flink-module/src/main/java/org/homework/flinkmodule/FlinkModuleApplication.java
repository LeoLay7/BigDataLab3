package org.homework.flinkmodule;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.homework.flinkmodule.NormalizedRecord;
import org.homework.flinkmodule.SnowflakeJdbcSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class FlinkModuleApplication {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkModuleApplication.class);

	private static final String UPSERT_SQL = """
		WITH payload AS (
			SELECT CAST(? AS jsonb) AS j
		),
		norm AS (
			SELECT
				COALESCE(NULLIF(j->>'customer_first_name', ''), 'Unknown') AS customer_first_name,
				COALESCE(NULLIF(j->>'customer_last_name', ''), 'Unknown') AS customer_last_name,
				CAST(COALESCE(NULLIF(j->>'customer_age', ''), '0') AS INT) AS customer_age,
				COALESCE(NULLIF(j->>'customer_email', ''), 'unknown.customer@example.com') AS customer_email,
				COALESCE(NULLIF(j->>'customer_country', ''), 'Unknown') AS customer_country,
				COALESCE(NULLIF(j->>'customer_postal_code', ''), '') AS customer_postal_code,
				COALESCE(NULLIF(j->>'customer_pet_type', ''), 'Unknown') AS customer_pet_type,
				COALESCE(NULLIF(j->>'customer_pet_name', ''), 'Unknown') AS customer_pet_name,
				COALESCE(NULLIF(j->>'customer_pet_breed', ''), 'Unknown') AS customer_pet_breed,

				COALESCE(NULLIF(j->>'seller_first_name', ''), 'Unknown') AS seller_first_name,
				COALESCE(NULLIF(j->>'seller_last_name', ''), 'Unknown') AS seller_last_name,
				COALESCE(NULLIF(j->>'seller_email', ''), 'unknown.seller@example.com') AS seller_email,
				COALESCE(NULLIF(j->>'seller_country', ''), 'Unknown') AS seller_country,
				COALESCE(NULLIF(j->>'seller_postal_code', ''), '') AS seller_postal_code,

				COALESCE(NULLIF(j->>'product_name', ''), 'Unknown product') AS product_name,
				COALESCE(NULLIF(j->>'product_category', ''), 'Unknown') AS product_category,
				CAST(COALESCE(NULLIF(j->>'product_price', ''), '0') AS NUMERIC(10,2)) AS product_price,
				CAST(COALESCE(NULLIF(j->>'product_quantity', ''), '0') AS INT) AS product_quantity,
				CAST(COALESCE(NULLIF(j->>'product_weight', ''), '0') AS NUMERIC(10,2)) AS product_weight,
				COALESCE(NULLIF(j->>'product_color', ''), 'Unknown') AS product_color,
				COALESCE(NULLIF(j->>'product_size', ''), 'Unknown') AS product_size,
				COALESCE(NULLIF(j->>'product_brand', ''), 'Unknown') AS product_brand,
				COALESCE(NULLIF(j->>'product_material', ''), 'Unknown') AS product_material,
				COALESCE(NULLIF(j->>'product_description', ''), '') AS product_description,
				CAST(COALESCE(NULLIF(j->>'product_rating', ''), '0') AS NUMERIC(3,1)) AS product_rating,
				CAST(COALESCE(NULLIF(j->>'product_reviews', ''), '0') AS INT) AS product_reviews,

				COALESCE(NULLIF(j->>'pet_category', ''), 'Unknown') AS pet_category,

				COALESCE(NULLIF(j->>'store_name', ''), 'Unknown store') AS store_name,
				COALESCE(NULLIF(j->>'store_location', ''), '') AS store_location,
				COALESCE(NULLIF(j->>'store_city', ''), 'Unknown') AS store_city,
				COALESCE(NULLIF(j->>'store_state', ''), '') AS store_state,
				COALESCE(NULLIF(j->>'store_country', ''), 'Unknown') AS store_country,
				COALESCE(NULLIF(j->>'store_phone', ''), 'unknown-store-phone') AS store_phone,
				COALESCE(NULLIF(j->>'store_email', ''), 'unknown.store@example.com') AS store_email,

				COALESCE(NULLIF(j->>'supplier_name', ''), 'Unknown supplier') AS supplier_name,
				COALESCE(NULLIF(j->>'supplier_contact', ''), 'Unknown contact') AS supplier_contact,
				COALESCE(NULLIF(j->>'supplier_email', ''), 'unknown.supplier@example.com') AS supplier_email,
				COALESCE(NULLIF(j->>'supplier_phone', ''), 'unknown-supplier-phone') AS supplier_phone,
				COALESCE(NULLIF(j->>'supplier_address', ''), '') AS supplier_address,
				COALESCE(NULLIF(j->>'supplier_city', ''), 'Unknown') AS supplier_city,
				COALESCE(NULLIF(j->>'supplier_country', ''), 'Unknown') AS supplier_country,

				TO_DATE(COALESCE(NULLIF(j->>'sale_date', ''), '1/1/1970'), 'FMMM/FMDD/YYYY') AS sale_date,
				TO_DATE(COALESCE(NULLIF(j->>'product_release_date', ''), '1/1/1970'), 'FMMM/FMDD/YYYY') AS release_date,
				TO_DATE(COALESCE(NULLIF(j->>'product_expiry_date', ''), '1/1/1970'), 'FMMM/FMDD/YYYY') AS expiry_date,

				CAST(COALESCE(NULLIF(j->>'sale_quantity', ''), '0') AS INT) AS sale_quantity,
				CAST(COALESCE(NULLIF(j->>'sale_total_price', ''), '0') AS NUMERIC(10,2)) AS sale_total_price
			FROM payload
		),
		unknown_city AS (
			INSERT INTO dim_city (city, state)
			VALUES ('Unknown', '')
			ON CONFLICT (city, state) DO NOTHING
			RETURNING city_id
		),
		customer_country_ins AS (
			INSERT INTO dim_country (country)
			SELECT customer_country FROM norm
			ON CONFLICT (country) DO NOTHING
			RETURNING country_id
		),
		seller_country_ins AS (
			INSERT INTO dim_country (country)
			SELECT seller_country FROM norm
			ON CONFLICT (country) DO NOTHING
			RETURNING country_id
		),
		store_country_ins AS (
			INSERT INTO dim_country (country)
			SELECT store_country FROM norm
			ON CONFLICT (country) DO NOTHING
			RETURNING country_id
		),
		supplier_country_ins AS (
			INSERT INTO dim_country (country)
			SELECT supplier_country FROM norm
			ON CONFLICT (country) DO NOTHING
			RETURNING country_id
		),
		store_city_ins AS (
			INSERT INTO dim_city (city, state)
			SELECT store_city, store_state FROM norm
			ON CONFLICT (city, state) DO NOTHING
			RETURNING city_id
		),
		supplier_city_ins AS (
			INSERT INTO dim_city (city, state)
			SELECT supplier_city, '' FROM norm
			ON CONFLICT (city, state) DO NOTHING
			RETURNING city_id
		),
		customer_location_ins AS (
			INSERT INTO dim_location (city_id, country_id, postal_code, address)
			SELECT
				(SELECT city_id FROM dim_city WHERE city = 'Unknown' AND state = ''),
				dc.country_id,
				n.customer_postal_code,
				''
			FROM norm n
			JOIN dim_country dc ON dc.country = n.customer_country
			ON CONFLICT (city_id, country_id, postal_code, address)
			DO NOTHING
			RETURNING location_id
		),
		seller_location_ins AS (
			INSERT INTO dim_location (city_id, country_id, postal_code, address)
			SELECT
				(SELECT city_id FROM dim_city WHERE city = 'Unknown' AND state = ''),
				dc.country_id,
				n.seller_postal_code,
				''
			FROM norm n
			JOIN dim_country dc ON dc.country = n.seller_country
			ON CONFLICT (city_id, country_id, postal_code, address)
			DO NOTHING
			RETURNING location_id
		),
		store_location_ins AS (
			INSERT INTO dim_location (city_id, country_id, postal_code, address)
			SELECT
				c.city_id,
				dc.country_id,
				'',
				n.store_location
			FROM norm n
			JOIN dim_city c ON c.city = n.store_city AND c.state = n.store_state
			JOIN dim_country dc ON dc.country = n.store_country
			ON CONFLICT (city_id, country_id, postal_code, address)
			DO NOTHING
			RETURNING location_id
		),
		supplier_location_ins AS (
			INSERT INTO dim_location (city_id, country_id, postal_code, address)
			SELECT
				c.city_id,
				dc.country_id,
				'',
				n.supplier_address
			FROM norm n
			JOIN dim_city c ON c.city = n.supplier_city AND c.state = ''
			JOIN dim_country dc ON dc.country = n.supplier_country
			ON CONFLICT (city_id, country_id, postal_code, address)
			DO NOTHING
			RETURNING location_id
		),
		pet_type_ins AS (
			INSERT INTO dim_pet_type (type)
			SELECT customer_pet_type FROM norm
			ON CONFLICT (type) DO UPDATE SET type = EXCLUDED.type
			RETURNING pet_type_id
		),
		pet_breed_ins AS (
			INSERT INTO dim_pet_breed (breed)
			SELECT customer_pet_breed FROM norm
			ON CONFLICT (breed) DO UPDATE SET breed = EXCLUDED.breed
			RETURNING pet_breed_id
		),
		pet_ins AS (
			INSERT INTO dim_pet (pet_type_id, pet_breed)
			SELECT
				pt.pet_type_id,
				pb.pet_breed_id
			FROM norm n
			JOIN dim_pet_type pt ON pt.type = n.customer_pet_type
			JOIN dim_pet_breed pb ON pb.breed = n.customer_pet_breed
			ON CONFLICT (pet_type_id, pet_breed)
			DO UPDATE SET pet_breed = EXCLUDED.pet_breed
			RETURNING pet_id
		),
		category_ins AS (
			INSERT INTO dim_category (category_name)
			SELECT product_category FROM norm
			ON CONFLICT (category_name) DO UPDATE SET category_name = EXCLUDED.category_name
			RETURNING category_id
		),
		pet_category_ins AS (
			INSERT INTO dim_pet_category (category_name)
			SELECT pet_category FROM norm
			ON CONFLICT (category_name) DO UPDATE SET category_name = EXCLUDED.category_name
			RETURNING pet_category_id
		),
		brand_ins AS (
			INSERT INTO dim_brand (brand_name)
			SELECT product_brand FROM norm
			ON CONFLICT (brand_name) DO UPDATE SET brand_name = EXCLUDED.brand_name
			RETURNING brand_id
		),
		sale_date_ins AS (
			INSERT INTO dim_date (full_date, year, month, day_of_week)
			SELECT sale_date,
				   EXTRACT(YEAR FROM sale_date)::INT,
				   EXTRACT(MONTH FROM sale_date)::INT,
				   EXTRACT(ISODOW FROM sale_date)::INT
			FROM norm
			ON CONFLICT (full_date) DO NOTHING
			RETURNING date_id
		),
		release_date_ins AS (
			INSERT INTO dim_date (full_date, year, month, day_of_week)
			SELECT release_date,
				   EXTRACT(YEAR FROM release_date)::INT,
				   EXTRACT(MONTH FROM release_date)::INT,
				   EXTRACT(ISODOW FROM release_date)::INT
			FROM norm
			ON CONFLICT (full_date) DO NOTHING
			RETURNING date_id
		),
		expiry_date_ins AS (
			INSERT INTO dim_date (full_date, year, month, day_of_week)
			SELECT expiry_date,
				   EXTRACT(YEAR FROM expiry_date)::INT,
				   EXTRACT(MONTH FROM expiry_date)::INT,
				   EXTRACT(ISODOW FROM expiry_date)::INT
			FROM norm
			ON CONFLICT (full_date) DO NOTHING
			RETURNING date_id
		),
		customer_ins AS (
			INSERT INTO dim_customer (first_name, last_name, age, email, location_id, pet_id, pet_name)
			SELECT
				n.customer_first_name,
				n.customer_last_name,
				n.customer_age,
				n.customer_email,
				l.location_id,
				p.pet_id,
				n.customer_pet_name
			FROM norm n
			JOIN dim_location l ON l.city_id = (SELECT city_id FROM dim_city WHERE city = 'Unknown' AND state = '')
				AND l.country_id = (SELECT country_id FROM dim_country WHERE country = n.customer_country)
				AND l.postal_code = n.customer_postal_code
				AND l.address = ''
			JOIN dim_pet p ON p.pet_type_id = (SELECT pet_type_id FROM dim_pet_type WHERE type = n.customer_pet_type)
				AND p.pet_breed = (SELECT pet_breed_id FROM dim_pet_breed WHERE breed = n.customer_pet_breed)
			ON CONFLICT (first_name, last_name, email, location_id, pet_id, pet_name)
			DO UPDATE SET age = EXCLUDED.age
			RETURNING customer_id
		),
		seller_ins AS (
			INSERT INTO dim_seller (first_name, last_name, email, location_id)
			SELECT
				n.seller_first_name,
				n.seller_last_name,
				n.seller_email,
				l.location_id
			FROM norm n
			JOIN dim_location l ON l.city_id = (SELECT city_id FROM dim_city WHERE city = 'Unknown' AND state = '')
				AND l.country_id = (SELECT country_id FROM dim_country WHERE country = n.seller_country)
				AND l.postal_code = n.seller_postal_code
				AND l.address = ''
			ON CONFLICT (first_name, last_name, email, location_id)
			DO UPDATE SET email = EXCLUDED.email
			RETURNING seller_id
		),
		product_ins AS (
			INSERT INTO dim_product (
				product_name, category_id, pet_category_id, brand_id, price, quantity,
				weight, color, size, material, description, rating, reviews,
				release_date_id, expiry_date_id
			)
			SELECT
				n.product_name,
				c.category_id,
				pc.pet_category_id,
				b.brand_id,
				n.product_price,
				n.product_quantity,
				n.product_weight,
				n.product_color,
				n.product_size,
				n.product_material,
				n.product_description,
				n.product_rating,
				n.product_reviews,
				dr.date_id,
				de.date_id
			FROM norm n
			JOIN dim_category c ON c.category_name = n.product_category
			JOIN dim_pet_category pc ON pc.category_name = n.pet_category
			JOIN dim_brand b ON b.brand_name = n.product_brand
			JOIN dim_date dr ON dr.full_date = n.release_date
			JOIN dim_date de ON de.full_date = n.expiry_date
			ON CONFLICT (product_name, category_id, pet_category_id, brand_id, release_date_id, expiry_date_id)
			DO UPDATE SET
				price = EXCLUDED.price,
				quantity = EXCLUDED.quantity,
				weight = EXCLUDED.weight,
				color = EXCLUDED.color,
				size = EXCLUDED.size,
				material = EXCLUDED.material,
				description = EXCLUDED.description,
				rating = EXCLUDED.rating,
				reviews = EXCLUDED.reviews
			RETURNING product_id
		),
		store_ins AS (
			INSERT INTO dim_store (store_name, location_id, phone, email)
			SELECT
				n.store_name,
				l.location_id,
				n.store_phone,
				n.store_email
			FROM norm n
			JOIN dim_location l ON l.city_id = (SELECT city_id FROM dim_city WHERE city = n.store_city AND state = n.store_state)
				AND l.country_id = (SELECT country_id FROM dim_country WHERE country = n.store_country)
				AND l.postal_code = ''
				AND l.address = n.store_location
			ON CONFLICT (store_name, location_id, phone, email)
			DO UPDATE SET email = EXCLUDED.email
			RETURNING store_id
		),
		supplier_ins AS (
			INSERT INTO dim_supplier (name, contact, email, phone, location_id)
			SELECT
				n.supplier_name,
				n.supplier_contact,
				n.supplier_email,
				n.supplier_phone,
				l.location_id
			FROM norm n
			JOIN dim_location l ON l.city_id = (SELECT city_id FROM dim_city WHERE city = n.supplier_city AND state = '')
				AND l.country_id = (SELECT country_id FROM dim_country WHERE country = n.supplier_country)
				AND l.postal_code = ''
				AND l.address = n.supplier_address
			ON CONFLICT (name, contact, email, phone, location_id)
			DO UPDATE SET contact = EXCLUDED.contact
			RETURNING supplier_id
		),
		fact_ins AS (
			INSERT INTO fact_sales (date_id, customer_id, seller_id, product_id, store_id, supplier_id, quantity, total_price)
			SELECT DISTINCT ON (d.date_id, c.customer_id, s.seller_id, p.product_id, st.store_id, sup.supplier_id)
				d.date_id,
				c.customer_id,
				s.seller_id,
				p.product_id,
				st.store_id,
				sup.supplier_id,
				n.sale_quantity,
				n.sale_total_price
			FROM norm n
			JOIN dim_date d ON d.full_date = n.sale_date
			JOIN dim_customer c ON c.first_name = n.customer_first_name
				AND c.last_name = n.customer_last_name
				AND c.email = n.customer_email
				AND c.pet_name = n.customer_pet_name
			JOIN dim_seller s ON s.first_name = n.seller_first_name
				AND s.last_name = n.seller_last_name
				AND s.email = n.seller_email
			JOIN dim_product p ON p.product_name = n.product_name
			JOIN dim_store st ON st.store_name = n.store_name
				AND st.phone = n.store_phone
			JOIN dim_supplier sup ON sup.name = n.supplier_name
				AND sup.email = n.supplier_email
				AND sup.phone = n.supplier_phone
			ORDER BY d.date_id, c.customer_id, s.seller_id, p.product_id, st.store_id, sup.supplier_id
			ON CONFLICT (date_id, customer_id, seller_id, product_id, store_id, supplier_id)
			DO UPDATE SET
				quantity = EXCLUDED.quantity,
				total_price = EXCLUDED.total_price
			RETURNING sale_id
		)
		SELECT 1;
		""";

	public static void main(String[] args) throws Exception {
		AppConfig config = AppConfig.load();
		LOG.info("Starting Flink job with config: kafkaBootstrapServers={}, kafkaTopic={}, kafkaGroupId={}, jdbcUrl={}, parallelism={}, checkpointIntervalMs={}, jdbcBatchSize={}, jdbcBatchIntervalMs={}, logEveryNMessages={}, previewLength={}",
			config.kafkaBootstrapServers,
			config.kafkaTopic,
			config.kafkaGroupId,
			config.jdbcUrl,
			config.parallelism,
			config.checkpointIntervalMs,
			config.jdbcBatchSize,
			config.jdbcBatchIntervalMs,
			config.logEveryNMessages,
			config.previewLength);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(config.parallelism);
		env.enableCheckpointing(config.checkpointIntervalMs);

		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", config.kafkaBootstrapServers);
		kafkaProperties.setProperty("group.id", config.kafkaGroupId);
		kafkaProperties.setProperty("auto.offset.reset", "earliest");

		FlinkKafkaConsumer<String> source = new FlinkKafkaConsumer<>(
			config.kafkaTopic,
			new SimpleStringSchema(),
			kafkaProperties
		);

		env
			.addSource(source)
			.name("kafka-source")
			.flatMap(new JsonValidationFlatMap(config.logEveryNMessages, config.previewLength))
			.name("json-validation")
			.flatMap(new JsonToNormalizedFlatMap())
			.name("to-normalized")
			.addSink(new SnowflakeJdbcSink(config))
			.name("snowflake-sequential-upsert");

		LOG.info("Submitting Flink execution graph: {}", "lab3-kafka-to-postgres-snowflake");
		env.execute("lab3-kafka-to-postgres-snowflake");
	}

	static class JsonToNormalizedFlatMap implements FlatMapFunction<String, NormalizedRecord> {

		@Override
		public void flatMap(String value, Collector<NormalizedRecord> out) {
			try {
				NormalizedRecord r = NormalizedRecord.fromJson(value);
				out.collect(r);
			} catch (IOException ex) {
				LOG.warn("Failed to parse JSON to NormalizedRecord, skipping: {}", ex.getMessage());
			}
		}
	}

	static class JsonValidationFlatMap implements FlatMapFunction<String, String> {

		private final AtomicLong totalSeen = new AtomicLong();
		private final AtomicLong totalValid = new AtomicLong();
		private final AtomicLong totalInvalid = new AtomicLong();
		private final int logEveryNMessages;
		private final int previewLength;

		JsonValidationFlatMap(int logEveryNMessages, int previewLength) {
			this.logEveryNMessages = Math.max(1, logEveryNMessages);
			this.previewLength = Math.max(32, previewLength);
		}

		@Override
		public void flatMap(String value, Collector<String> out) {
			long seen = totalSeen.incrementAndGet();
			String trimmed = value == null ? "" : value.trim();
			if (trimmed.startsWith("{") && trimmed.endsWith("}") && trimmed.contains(":")) {
				long valid = totalValid.incrementAndGet();
				out.collect(trimmed);
				if (seen == 1 || seen % logEveryNMessages == 0) {
					LOG.info("Flink validation progress: seen={}, valid={}, invalid={}", seen, valid, totalInvalid.get());
				}
			} else {
				long invalid = totalInvalid.incrementAndGet();
				LOG.warn("Skipping malformed JSON-like message, seen={}, invalid={}, preview='{}'",
					seen,
					invalid,
					preview(value, previewLength));
			}
		}

		private static String preview(String value, int maxLen) {
			if (value == null) {
				return "<null>";
			}

			String normalized = value.replace('\n', ' ').replace('\r', ' ');
			return normalized.length() <= maxLen ? normalized : normalized.substring(0, maxLen) + "...";
		}
	}

	static class AppConfig implements java.io.Serializable {
		private static final long serialVersionUID = 1L;
		final String kafkaBootstrapServers;
		final String kafkaTopic;
		final String kafkaGroupId;
		final String jdbcUrl;
		final String jdbcUser;
		final String jdbcPassword;
		final int parallelism;
		final long checkpointIntervalMs;
		final int jdbcBatchSize;
		final long jdbcBatchIntervalMs;
		final int logEveryNMessages;
		final int previewLength;

		AppConfig(
			String kafkaBootstrapServers,
			String kafkaTopic,
			String kafkaGroupId,
			String jdbcUrl,
			String jdbcUser,
			String jdbcPassword,
			int parallelism,
			long checkpointIntervalMs,
			int jdbcBatchSize,
			long jdbcBatchIntervalMs,
			int logEveryNMessages,
			int previewLength
		) {
			this.kafkaBootstrapServers = kafkaBootstrapServers;
			this.kafkaTopic = kafkaTopic;
			this.kafkaGroupId = kafkaGroupId;
			this.jdbcUrl = jdbcUrl;
			this.jdbcUser = jdbcUser;
			this.jdbcPassword = jdbcPassword;
			this.parallelism = parallelism;
			this.checkpointIntervalMs = checkpointIntervalMs;
			this.jdbcBatchSize = jdbcBatchSize;
			this.jdbcBatchIntervalMs = jdbcBatchIntervalMs;
			this.logEveryNMessages = logEveryNMessages;
			this.previewLength = previewLength;
		}

		static AppConfig load() {
			Properties properties = new Properties();
			try (InputStream inputStream = FlinkModuleApplication.class
				.getClassLoader()
				.getResourceAsStream("application.properties")) {
				if (inputStream != null) {
					properties.load(inputStream);
				}
			} catch (IOException ex) {
				throw new IllegalStateException("Unable to load application.properties", ex);
			}

			return new AppConfig(
				get(properties, "kafka.bootstrap.servers", "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
				get(properties, "kafka.topic", "KAFKA_TOPIC", "mock-data"),
				get(properties, "kafka.group.id", "KAFKA_GROUP_ID", "flink-lab3-consumer"),
				get(properties, "jdbc.url", "JDBC_URL", "jdbc:postgresql://postgres:5555/lab3"),
				get(properties, "jdbc.user", "JDBC_USER", "student"),
				get(properties, "jdbc.password", "JDBC_PASSWORD", "student"),
				Integer.parseInt(get(properties, "flink.parallelism", "FLINK_PARALLELISM", "1")),
				Long.parseLong(get(properties, "flink.checkpoint.interval.ms", "FLINK_CHECKPOINT_INTERVAL_MS", "15000")),
				Integer.parseInt(get(properties, "jdbc.batch.size", "JDBC_BATCH_SIZE", "25")),
				Long.parseLong(get(properties, "jdbc.batch.interval.ms", "JDBC_BATCH_INTERVAL_MS", "1000")),
				Integer.parseInt(get(properties, "flink.log.every.n.messages", "FLINK_LOG_EVERY_N_MESSAGES", "500")),
				Integer.parseInt(get(properties, "flink.log.preview.length", "FLINK_LOG_PREVIEW_LENGTH", "220"))
			);
		}

		private static String get(Properties properties, String key, String envKey, String defaultValue) {
			String envValue = System.getenv(envKey);
			if (envValue != null && !envValue.isBlank()) {
				return envValue;
			}

			return properties.getProperty(key, defaultValue);
		}
	}
}
