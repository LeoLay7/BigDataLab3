package org.homework.flinkmodule;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class SnowflakeJdbcSink extends RichSinkFunction<NormalizedRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(SnowflakeJdbcSink.class);

    private final FlinkModuleApplication.AppConfig config;

    private transient Connection connection;

    public SnowflakeJdbcSink(FlinkModuleApplication.AppConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("org.postgresql.Driver");
        this.connection = DriverManager.getConnection(config.jdbcUrl, config.jdbcUser, config.jdbcPassword);
        this.connection.setAutoCommit(false);
        LOG.info("SnowflakeJdbcSink opened connection to {}", config.jdbcUrl);
    }

    @Override
    public void invoke(NormalizedRecord r, Context context) throws Exception {
        if (r == null) {
            LOG.warn("Received null NormalizedRecord, skipping");
            return;
        }
        // Sequential upserts using helper methods
        Integer unknownCityId = getOrInsertCity("Unknown", "");

        Integer customerCountryId = getOrInsertCountry(r.customerCountry);
        Integer sellerCountryId = getOrInsertCountry(r.sellerCountry);
        Integer storeCountryId = getOrInsertCountry(r.storeCountry);
        Integer supplierCountryId = getOrInsertCountry(r.supplierCountry);

        Integer storeCityId = getOrInsertCity(r.storeCity, r.storeState);
        Integer supplierCityId = getOrInsertCity(r.supplierCity, "");

        Integer customerLocationId = getOrInsertLocation(unknownCityId, customerCountryId, r.customerPostalCode, "");
        Integer sellerLocationId = getOrInsertLocation(unknownCityId, sellerCountryId, r.sellerPostalCode, "");
        Integer storeLocationId = getOrInsertLocation(storeCityId, storeCountryId, "", r.storeLocation);
        Integer supplierLocationId = getOrInsertLocation(supplierCityId, supplierCountryId, "", r.supplierAddress);

        Integer petTypeId = getOrInsertPetType(r.customerPetType);
        Integer petBreedId = getOrInsertPetBreed(r.customerPetBreed);
        Integer petId = getOrInsertPet(petTypeId, petBreedId);

        Integer categoryId = getOrInsertCategory(r.productCategory);
        Integer petCategoryId = getOrInsertPetCategory(r.petCategory);
        Integer brandId = getOrInsertBrand(r.productBrand);

        Integer releaseDateId = getOrInsertDate(r.productReleaseDate);
        Integer expiryDateId = getOrInsertDate(r.productExpiryDate);
        Integer dateId = getOrInsertDate(r.saleDate);

        Integer customerId = getOrInsertCustomer(r.customerFirstName, r.customerLastName, r.customerAge, r.customerEmail, customerLocationId, petId, r.customerPetName);
        Integer sellerId = getOrInsertSeller(r.sellerFirstName, r.sellerLastName, r.sellerEmail, sellerLocationId);
        Integer productId = getOrInsertProduct(r.productName, categoryId, petCategoryId, brandId, r.productPrice, r.productQuantity, r.productWeight, releaseDateId, expiryDateId, r.productColor, r.productSize, r.productMaterial, r.productDescription, r.productRating, r.productReviews);
        Integer storeId = getOrInsertStore(r.storeName, storeLocationId, r.storePhone, r.storeEmail);
        Integer supplierId = getOrInsertSupplier(r.supplierName, r.supplierContact, r.supplierEmail, r.supplierPhone, supplierLocationId);

        // Upsert fact
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO fact_sales(date_id, customer_id, seller_id, product_id, store_id, supplier_id, quantity, total_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (date_id, customer_id, seller_id, product_id, store_id, supplier_id) DO UPDATE SET quantity = EXCLUDED.quantity, total_price = EXCLUDED.total_price")) {
            ps.setObject(1, dateId, Types.INTEGER);
            ps.setObject(2, customerId, Types.INTEGER);
            ps.setObject(3, sellerId, Types.INTEGER);
            ps.setObject(4, productId, Types.INTEGER);
            ps.setObject(5, storeId, Types.INTEGER);
            ps.setObject(6, supplierId, Types.INTEGER);
            ps.setInt(7, r.saleQuantity != null ? r.saleQuantity : 0);
            ps.setBigDecimal(8, r.saleTotalPrice != null ? r.saleTotalPrice : new java.math.BigDecimal("0"));
            ps.executeUpdate();
        }

        connection.commit();
    }

    private Integer getOrInsertCountry(String country) throws SQLException {
        if (country == null) country = "Unknown";
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_country(country) VALUES (?) ON CONFLICT (country) DO NOTHING")) {
            ps.setString(1, country);
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT country_id FROM dim_country WHERE country = ?")) {
            q.setString(1, country);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertCity(String city, String state) throws SQLException {
        if (city == null || city.isBlank()) city = "Unknown";
        if (state == null) state = "";
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_city(city, state) VALUES (?, ?) ON CONFLICT (city, state) DO NOTHING")) {
            ps.setString(1, city);
            ps.setString(2, state);
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT city_id FROM dim_city WHERE city = ? AND state = ?")) {
            q.setString(1, city);
            q.setString(2, state);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertLocation(Integer cityId, Integer countryId, String postalCode, String address) throws SQLException {
        if (cityId == null) cityId = getOrInsertCity("Unknown", "");
        if (countryId == null) countryId = getOrInsertCountry("Unknown");
        if (postalCode == null) postalCode = "";
        if (address == null) address = "";
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_location(city_id, country_id, postal_code, address) VALUES (?, ?, ?, ?) ON CONFLICT (city_id, country_id, postal_code, address) DO NOTHING")) {
            ps.setObject(1, cityId, Types.INTEGER);
            ps.setObject(2, countryId, Types.INTEGER);
            ps.setString(3, postalCode);
            ps.setString(4, address);
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT location_id FROM dim_location WHERE city_id = ? AND country_id = ? AND postal_code = ? AND address = ?")) {
            q.setObject(1, cityId, Types.INTEGER);
            q.setObject(2, countryId, Types.INTEGER);
            q.setString(3, postalCode);
            q.setString(4, address);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertPetType(String type) throws SQLException {
        if (type == null) type = "Unknown";
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_pet_type(type) VALUES (?) ON CONFLICT (type) DO NOTHING")) {
            ps.setString(1, type);
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT pet_type_id FROM dim_pet_type WHERE type = ?")) {
            q.setString(1, type);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertPetBreed(String breed) throws SQLException {
        if (breed == null) breed = "Unknown";
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_pet_breed(breed) VALUES (?) ON CONFLICT (breed) DO NOTHING")) {
            ps.setString(1, breed);
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT pet_breed_id FROM dim_pet_breed WHERE breed = ?")) {
            q.setString(1, breed);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertPet(Integer petTypeId, Integer petBreedId) throws SQLException {
        if (petTypeId == null) petTypeId = getOrInsertPetType("Unknown");
        if (petBreedId == null) petBreedId = getOrInsertPetBreed("Unknown");
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_pet(pet_type_id, pet_breed) VALUES (?, ?) ON CONFLICT (pet_type_id, pet_breed) DO NOTHING")) {
            ps.setObject(1, petTypeId, Types.INTEGER);
            ps.setObject(2, petBreedId, Types.INTEGER);
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT pet_id FROM dim_pet WHERE pet_type_id = ? AND pet_breed = ?")) {
            q.setObject(1, petTypeId, Types.INTEGER);
            q.setObject(2, petBreedId, Types.INTEGER);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertCategory(String category) throws SQLException {
        if (category == null) category = "Unknown";
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_category(category_name) VALUES (?) ON CONFLICT (category_name) DO NOTHING")) {
            ps.setString(1, category);
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT category_id FROM dim_category WHERE category_name = ?")) {
            q.setString(1, category);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertPetCategory(String petCategory) throws SQLException {
        if (petCategory == null) petCategory = "Unknown";
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_pet_category(category_name) VALUES (?) ON CONFLICT (category_name) DO NOTHING")) {
            ps.setString(1, petCategory);
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT pet_category_id FROM dim_pet_category WHERE category_name = ?")) {
            q.setString(1, petCategory);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertBrand(String brand) throws SQLException {
        if (brand == null) brand = "Unknown";
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_brand(brand_name) VALUES (?) ON CONFLICT (brand_name) DO NOTHING")) {
            ps.setString(1, brand);
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT brand_id FROM dim_brand WHERE brand_name = ?")) {
            q.setString(1, brand);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertDate(java.time.LocalDate date) throws SQLException {
        if (date == null) date = java.time.LocalDate.of(1970,1,1);
        Date sqlDate = Date.valueOf(date);
        // Compute year/month/day in Java to avoid ambiguity in SQL extract() overloads
        int year = date.getYear();
        int month = date.getMonthValue();
        int dayOfWeek = date.getDayOfWeek().getValue(); // ISO: Monday=1 .. Sunday=7
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_date(full_date, year, month, day_of_week) VALUES (?, ?, ?, ?) ON CONFLICT (full_date) DO NOTHING")) {
            ps.setDate(1, sqlDate);
            ps.setInt(2, year);
            ps.setInt(3, month);
            ps.setInt(4, dayOfWeek);
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT date_id FROM dim_date WHERE full_date = ?")) {
            q.setDate(1, sqlDate);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertCustomer(String firstName, String lastName, Integer age, String email, Integer locationId, Integer petId, String petName) throws SQLException {
        if (firstName == null) firstName = "Unknown";
        if (lastName == null) lastName = "Unknown";
        if (email == null) email = "unknown.customer@example.com";
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_customer(first_name, last_name, age, email, location_id, pet_id, pet_name) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (first_name, last_name, email, location_id, pet_id, pet_name) DO UPDATE SET age = EXCLUDED.age")) {
            ps.setString(1, firstName);
            ps.setString(2, lastName);
            ps.setInt(3, age != null ? age : 0);
            ps.setString(4, email);
            if (locationId != null) ps.setObject(5, locationId, Types.INTEGER); else ps.setNull(5, Types.INTEGER);
            if (petId != null) ps.setObject(6, petId, Types.INTEGER); else ps.setNull(6, Types.INTEGER);
            ps.setString(7, petName != null ? petName : "");
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT customer_id FROM dim_customer WHERE first_name = ? AND last_name = ? AND email = ?")) {
            q.setString(1, firstName);
            q.setString(2, lastName);
            q.setString(3, email);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertSeller(String firstName, String lastName, String email, Integer locationId) throws SQLException {
        if (firstName == null) firstName = "Unknown";
        if (lastName == null) lastName = "Unknown";
        if (email == null) email = "unknown.seller@example.com";
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_seller(first_name, last_name, email, location_id) VALUES (?, ?, ?, ?) ON CONFLICT (first_name, last_name, email, location_id) DO UPDATE SET email = EXCLUDED.email")) {
            ps.setString(1, firstName);
            ps.setString(2, lastName);
            ps.setString(3, email);
            if (locationId != null) ps.setObject(4, locationId, Types.INTEGER); else ps.setNull(4, Types.INTEGER);
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT seller_id FROM dim_seller WHERE first_name = ? AND last_name = ? AND email = ?")) {
            q.setString(1, firstName);
            q.setString(2, lastName);
            q.setString(3, email);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertProduct(
            String name,
            Integer categoryId,
            Integer petCategoryId,
            Integer brandId,
            java.math.BigDecimal price,
            Integer quantity,
            java.math.BigDecimal weight,
            Integer releaseDateId,
            Integer expiryDateId,
            String color,
            String size,
            String material,
            String description,
            java.math.BigDecimal rating,
            Integer reviews
    ) throws SQLException {
        if (name == null) name = "Unknown product";
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO dim_product(product_name, category_id, pet_category_id, brand_id, price, quantity, weight, color, size, material, description, rating, reviews, release_date_id, expiry_date_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (product_name, category_id, pet_category_id, brand_id, release_date_id, expiry_date_id) DO UPDATE SET price = EXCLUDED.price, quantity = EXCLUDED.quantity, weight = EXCLUDED.weight, color = EXCLUDED.color, size = EXCLUDED.size, material = EXCLUDED.material, description = EXCLUDED.description, rating = EXCLUDED.rating, reviews = EXCLUDED.reviews"
        )) {
            ps.setString(1, name);
            if (categoryId != null) ps.setObject(2, categoryId, Types.INTEGER); else ps.setNull(2, Types.INTEGER);
            if (petCategoryId != null) ps.setObject(3, petCategoryId, Types.INTEGER); else ps.setNull(3, Types.INTEGER);
            if (brandId != null) ps.setObject(4, brandId, Types.INTEGER); else ps.setNull(4, Types.INTEGER);
            ps.setBigDecimal(5, price != null ? price : new java.math.BigDecimal("0"));
            ps.setInt(6, quantity != null ? quantity : 0);
            ps.setBigDecimal(7, weight != null ? weight : new java.math.BigDecimal("0"));
            ps.setString(8, color != null ? color : "");
            ps.setString(9, size != null ? size : "");
            ps.setString(10, material != null ? material : "");
            ps.setString(11, description != null ? description : "");
            ps.setBigDecimal(12, rating != null ? rating : new java.math.BigDecimal("0"));
            ps.setInt(13, reviews != null ? reviews : 0);
            if (releaseDateId != null) ps.setObject(14, releaseDateId, Types.INTEGER); else ps.setNull(14, Types.INTEGER);
            if (expiryDateId != null) ps.setObject(15, expiryDateId, Types.INTEGER); else ps.setNull(15, Types.INTEGER);
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT product_id FROM dim_product WHERE product_name = ?")) {
            q.setString(1, name);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertStore(String storeName, Integer locationId, String phone, String email) throws SQLException {
        if (storeName == null) storeName = "Unknown store";
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_store(store_name, location_id, phone, email) VALUES (?, ?, ?, ?) ON CONFLICT (store_name, location_id, phone, email) DO UPDATE SET email = EXCLUDED.email")) {
            ps.setString(1, storeName);
            if (locationId != null) ps.setObject(2, locationId, Types.INTEGER); else ps.setNull(2, Types.INTEGER);
            ps.setString(3, phone != null ? phone : "");
            ps.setString(4, email != null ? email : "");
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT store_id FROM dim_store WHERE store_name = ?")) {
            q.setString(1, storeName);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    private Integer getOrInsertSupplier(String name, String contact, String email, String phone, Integer locationId) throws SQLException {
        if (name == null) name = "Unknown supplier";
        try (PreparedStatement ps = connection.prepareStatement("INSERT INTO dim_supplier(name, contact, email, phone, location_id) VALUES (?, ?, ?, ?, ?) ON CONFLICT (name, contact, email, phone, location_id) DO UPDATE SET contact = EXCLUDED.contact")) {
            ps.setString(1, name);
            ps.setString(2, contact != null ? contact : "");
            ps.setString(3, email != null ? email : "");
            ps.setString(4, phone != null ? phone : "");
            if (locationId != null) ps.setObject(5, locationId, Types.INTEGER); else ps.setNull(5, Types.INTEGER);
            ps.executeUpdate();
        }
        try (PreparedStatement q = connection.prepareStatement("SELECT supplier_id FROM dim_supplier WHERE name = ?")) {
            q.setString(1, name);
            try (ResultSet rs = q.executeQuery()) {
                if (rs.next()) return rs.getInt(1);
            }
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (Exception ex) {
                LOG.warn("Error closing JDBC connection: {}", ex.getMessage());
            }
        }
        super.close();
    }
}
