package org.homework.dataproxyservice.repository;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Repository
public class MockDataRepository {

    private static final String[] COLUMNS = {
            "id",
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "customer_country",
            "customer_postal_code",
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed",
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country",
            "seller_postal_code",
            "product_name",
            "product_category",
            "product_price",
            "product_quantity",
            "sale_date",
            "sale_customer_id",
            "sale_seller_id",
            "sale_product_id",
            "sale_quantity",
            "sale_total_price",
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email",
            "pet_category",
            "product_weight",
            "product_color",
            "product_size",
            "product_brand",
            "product_material",
            "product_description",
            "product_rating",
            "product_reviews",
            "product_release_date",
            "product_expiry_date",
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country"
    };

    private static final String SELECT_SQL = "SELECT " + String.join(", ", COLUMNS) + " FROM mock_data";

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional(readOnly = true)
    public List<Map<String, Object>> findAllRows() {
        @SuppressWarnings("unchecked")
        List<Object[]> result = entityManager.createNativeQuery(SELECT_SQL).getResultList();

        List<Map<String, Object>> rows = new ArrayList<>(result.size());
        for (Object[] row : result) {
            Map<String, Object> values = new LinkedHashMap<>(COLUMNS.length);
            for (int i = 0; i < COLUMNS.length; i++) {
                values.put(COLUMNS[i], i < row.length ? row[i] : null);
            }
            rows.add(values);
        }
        return rows;
    }
}

