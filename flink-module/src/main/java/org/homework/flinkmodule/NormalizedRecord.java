package org.homework.flinkmodule;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;

public class NormalizedRecord {
    public String customerFirstName;
    public String customerLastName;
    public Integer customerAge;
    public String customerEmail;
    public String customerCountry;
    public String customerPostalCode;
    public String customerPetType;
    public String customerPetName;
    public String customerPetBreed;

    public String sellerFirstName;
    public String sellerLastName;
    public String sellerEmail;
    public String sellerCountry;
    public String sellerPostalCode;

    public String productName;
    public String productCategory;
    public BigDecimal productPrice;
    public Integer productQuantity;
    public BigDecimal productWeight;
    public String productBrand;
    public String productColor;
    public String productSize;
    public String productMaterial;
    public String productDescription;
    public BigDecimal productRating;
    public Integer productReviews;
    public LocalDate productReleaseDate;
    public LocalDate productExpiryDate;
    public String petCategory;

    // store fields
    public String storeName;
    public String storeLocation;
    public String storeCity;
    public String storeState;
    public String storeCountry;
    public String storePhone;
    public String storeEmail;

    // supplier fields
    public String supplierName;
    public String supplierContact;
    public String supplierEmail;
    public String supplierPhone;
    public String supplierAddress;
    public String supplierCity;
    public String supplierCountry;

    public LocalDate saleDate;
    public Integer saleQuantity;
    public BigDecimal saleTotalPrice;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static NormalizedRecord fromJson(String json) throws IOException {
        JsonNode j = MAPPER.readTree(json);
        NormalizedRecord r = new NormalizedRecord();
        r.customerFirstName = textOrDefault(j, "customer_first_name", "Unknown");
        r.customerLastName = textOrDefault(j, "customer_last_name", "Unknown");
        r.customerAge = intOrDefault(j, "customer_age", 0);
        r.customerEmail = textOrDefault(j, "customer_email", "unknown.customer@example.com");
        r.customerCountry = textOrDefault(j, "customer_country", "Unknown");
        r.customerPostalCode = textOrDefault(j, "customer_postal_code", "");
        r.customerPetType = textOrDefault(j, "customer_pet_type", "Unknown");
        r.customerPetName = textOrDefault(j, "customer_pet_name", "Unknown");
        r.customerPetBreed = textOrDefault(j, "customer_pet_breed", "Unknown");

        r.sellerFirstName = textOrDefault(j, "seller_first_name", "Unknown");
        r.sellerLastName = textOrDefault(j, "seller_last_name", "Unknown");
        r.sellerEmail = textOrDefault(j, "seller_email", "unknown.seller@example.com");
        r.sellerCountry = textOrDefault(j, "seller_country", "Unknown");
        r.sellerPostalCode = textOrDefault(j, "seller_postal_code", "");

        r.productName = textOrDefault(j, "product_name", "Unknown product");
        r.productCategory = textOrDefault(j, "product_category", "Unknown");
        r.productPrice = decimalOrDefault(j, "product_price", BigDecimal.ZERO);
        r.productQuantity = intOrDefault(j, "product_quantity", 0);
        r.productWeight = decimalOrDefault(j, "product_weight", BigDecimal.ZERO);
        r.productBrand = textOrDefault(j, "product_brand", "Unknown");
        r.productColor = textOrDefault(j, "product_color", "Unknown");
        r.productSize = textOrDefault(j, "product_size", "Unknown");
        r.productMaterial = textOrDefault(j, "product_material", "Unknown");
        r.productDescription = textOrDefault(j, "product_description", "");
        r.productRating = decimalOrDefault(j, "product_rating", BigDecimal.ZERO);
        r.productReviews = intOrDefault(j, "product_reviews", 0);

        r.saleDate = dateOrDefault(j, "sale_date");
        r.productReleaseDate = dateOrDefault(j, "product_release_date");
        r.productExpiryDate = dateOrDefault(j, "product_expiry_date");
        r.petCategory = textOrDefault(j, "pet_category", "Unknown");

        r.saleQuantity = intOrDefault(j, "sale_quantity", 0);
        r.saleTotalPrice = decimalOrDefault(j, "sale_total_price", BigDecimal.ZERO);

        // store fields
        r.storeName = textOrDefault(j, "store_name", "Unknown store");
        r.storeLocation = textOrDefault(j, "store_location", "");
        r.storeCity = textOrDefault(j, "store_city", "Unknown");
        r.storeState = textOrDefault(j, "store_state", "");
        r.storeCountry = textOrDefault(j, "store_country", "Unknown");
        r.storePhone = textOrDefault(j, "store_phone", "unknown-store-phone");
        r.storeEmail = textOrDefault(j, "store_email", "unknown.store@example.com");

        // supplier fields
        r.supplierName = textOrDefault(j, "supplier_name", "Unknown supplier");
        r.supplierContact = textOrDefault(j, "supplier_contact", "Unknown contact");
        r.supplierEmail = textOrDefault(j, "supplier_email", "unknown.supplier@example.com");
        r.supplierPhone = textOrDefault(j, "supplier_phone", "");
        r.supplierAddress = textOrDefault(j, "supplier_address", "");
        r.supplierCity = textOrDefault(j, "supplier_city", "Unknown");
        r.supplierCountry = textOrDefault(j, "supplier_country", "Unknown");

        return r;
    }

    private static String textOrDefault(JsonNode j, String key, String def) {
        JsonNode n = j.get(key);
        if (n == null || n.isNull()) return def;
        String t = n.asText("");
        return t.isBlank() ? def : t;
    }

    private static Integer intOrDefault(JsonNode j, String key, int def) {
        JsonNode n = j.get(key);
        if (n == null || n.isNull()) return def;
        try {
            return Integer.parseInt(n.asText());
        } catch (Exception ex) {
            return def;
        }
    }

    private static BigDecimal decimalOrDefault(JsonNode j, String key, BigDecimal def) {
        JsonNode n = j.get(key);
        if (n == null || n.isNull()) return def;
        try {
            return new BigDecimal(n.asText());
        } catch (Exception ex) {
            return def;
        }
    }

    private static LocalDate dateOrDefault(JsonNode j, String key) {
        JsonNode n = j.get(key);
        if (n == null || n.isNull() || n.asText().isBlank()) return LocalDate.of(1970, 1, 1);
        try {
            String[] parts = n.asText().split("/");
            int m = Integer.parseInt(parts[0]);
            int d = Integer.parseInt(parts[1]);
            int y = Integer.parseInt(parts[2]);
            return LocalDate.of(y, m, d);
        } catch (Exception ex) {
            return LocalDate.of(1970, 1, 1);
        }
    }
}
