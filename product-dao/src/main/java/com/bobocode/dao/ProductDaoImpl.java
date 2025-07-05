package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ProductDaoImpl implements ProductDao {

    public static final String SAVE_PRODUCT_SQL = "INSERT INTO products " +
            "(name, producer, price, expiration_date) " +
            "VALUES (?, ?, ?, ?)";
    public static final String FIND_ALL_PRODUCTS_SQL = "SELECT * FROM products";

    private DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        try (Connection con = dataSource.getConnection()) {
            PreparedStatement ps = con.prepareStatement(SAVE_PRODUCT_SQL, Statement.RETURN_GENERATED_KEYS);
            fillPreparedStatementWithData(ps, product);
            executeUpdate(product, ps);
            insertGeneratedKeyIntoProduct(ps, product);
        } catch (SQLException e) {
            throw new DaoOperationException("Error while saving Product: " + product, e);
        }
//        throw new UnsupportedOperationException("None of these methods will work unless you implement them!");// todo
    }

    @Override
    public List<Product> findAll() {
        try (Connection con = dataSource.getConnection()) {
            List<Product> products = new ArrayList<>();
            Statement s = con.createStatement();
            ResultSet rs = s.executeQuery(FIND_ALL_PRODUCTS_SQL);
            fillProductListWithResults(rs, products);
            return products;
        } catch (SQLException e) {
            throw new DaoOperationException("Error while finding all products", e);
        }
    }

    @Override
    public Product findOne(Long id) {
        throw new UnsupportedOperationException("None of these methods will work unless you implement them!");// todo
    }

    @Override
    public void update(Product product) {
        throw new UnsupportedOperationException("None of these methods will work unless you implement them!");// todo
    }

    @Override
    public void remove(Product product) {
        throw new UnsupportedOperationException("None of these methods will work unless you implement them!");// todo
    }

    private void fillPreparedStatementWithData(PreparedStatement ps, Product product) throws SQLException {
        ps.setString(1, product.getName());
        ps.setString(2, product.getProducer());
        ps.setBigDecimal(3, product.getPrice());
        ps.setDate(4, Date.valueOf(product.getExpirationDate()));
    }

    private void executeUpdate(Product product, PreparedStatement ps) throws SQLException {
        int inserted = ps.executeUpdate();
        if (inserted == 0) {
            throw new DaoOperationException("Product: " + product + " was not inserted");
        }
    }

    private void insertGeneratedKeyIntoProduct(PreparedStatement ps, Product product) throws SQLException {
        ResultSet generatedKeys = ps.getGeneratedKeys();
        if (generatedKeys.next()) {
            product.setId(generatedKeys.getLong("id"));
        } else {
            throw new DaoOperationException("Error while fetching generated key for Product: " + product);
        }
    }

    private void fillProductListWithResults(ResultSet rs, List<Product> products) throws SQLException {
        while (rs.next()) {
            Product p = new Product();
            p.setId(rs.getLong("id"));
            p.setName(rs.getString("name"));
            p.setProducer(rs.getString("producer"));
            p.setPrice(rs.getBigDecimal("price"));
            p.setExpirationDate(rs.getDate("expiration_date").toLocalDate());
            p.setCreationTime(rs.getTimestamp("creation_time").toLocalDateTime());
            products.add(p);
        }
    }

}
