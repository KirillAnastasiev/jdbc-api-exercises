package com.bobocode.dao;

import com.bobocode.exception.DaoOperationException;
import com.bobocode.model.Product;

import javax.sql.DataSource;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ProductDaoImpl implements ProductDao {

    public static final String SAVE_PRODUCT_SQL = "INSERT INTO products " +
            "(name, producer, price, expiration_date) VALUES (?, ?, ?, ?)";
    public static final String FIND_ALL_PRODUCTS_SQL = "SELECT * FROM products";
    public static final String FIND_PRODUCT_BY_ID_SQL = "SELECT * FROM products WHERE id = ?";
    public static final String UPDATE_PRODUCT_SQL = "UPDATE products SET name = ?, " +
            "producer = ?, price = ?, expiration_date = ? WHERE id = ?";
    public static final String REMOVE_PRODUCT_BY_ID = "DELETE FROM products WHERE id = ?";

    private DataSource dataSource;

    public ProductDaoImpl(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void save(Product product) {
        try (Connection con = dataSource.getConnection()) {
            PreparedStatement ps = con.prepareStatement(SAVE_PRODUCT_SQL, Statement.RETURN_GENERATED_KEYS);
            fillPreparedStatementWithDataReturningIndex(ps, product);
            executeUpdate(ps, String.format("Product %s was not inserted", product));
            insertGeneratedKeyIntoProduct(ps, product);
        } catch (SQLException e) {
            throw new DaoOperationException("Error saving product: " + product, e);
        }
    }

    @Override
    public List<Product> findAll() {
        try (Connection con = dataSource.getConnection()) {
            Statement s = con.createStatement();
            ResultSet rs = s.executeQuery(FIND_ALL_PRODUCTS_SQL);
            return getProductStreamFromResultSet(rs).collect(Collectors.toList());
        } catch (SQLException e) {
            throw new DaoOperationException("Error while finding all products", e);
        }
    }

    @Override
    public Product findOne(Long id) {
        try (Connection con = dataSource.getConnection()) {
            PreparedStatement ps = con.prepareStatement(FIND_PRODUCT_BY_ID_SQL);
            ps.setLong(1, id);
            ResultSet rs = ps.executeQuery();
            Product p = fetchProduct(id, rs);
            return p;
        } catch (SQLException e) {
            throw new DaoOperationException("Error finding product with id = " + id, e);
        }
    }

    @Override
    public void update(Product product) {
        try (Connection con = dataSource.getConnection()) {
            checkIfProductExists(product);
            PreparedStatement ps = con.prepareStatement(UPDATE_PRODUCT_SQL);
            int idx = fillPreparedStatementWithDataReturningIndex(ps, product);
            ps.setLong(idx, product.getId());
            executeUpdate(ps, String.format("Product with id = %d does not exist", product.getId()));
        } catch (SQLException e) {
            throw new DaoOperationException("Error updating product " + product, e);
        }
    }

    @Override
    public void remove(Product product) {
        try (Connection con = dataSource.getConnection()) {
            checkIfProductExists(product);
            PreparedStatement ps = con.prepareStatement(REMOVE_PRODUCT_BY_ID);
            ps.setLong(1, product.getId());
            executeUpdate(ps, String.format("Product with id = %d does not exist", product.getId()));
        } catch (SQLException e) {
            throw new DaoOperationException("Error removing product " + product);
        }
    }

    private int fillPreparedStatementWithDataReturningIndex(PreparedStatement ps, Product product) throws SQLException {
        int idx = 1;
        ps.setString(idx++, product.getName());
        ps.setString(idx++, product.getProducer());
        ps.setBigDecimal(idx++, product.getPrice());
        ps.setDate(idx++, Date.valueOf(product.getExpirationDate()));
        return idx;
    }

    private void executeUpdate(PreparedStatement ps, String errMsg) throws SQLException {
        int updated = ps.executeUpdate();
        if (updated == 0) {
            throw new DaoOperationException(errMsg);
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

    private Product fetchProduct(Long id, ResultSet rs) throws SQLException {
        if (rs.next()) {
            return fetchProductFromResultSet(rs);
        } else {
            throw new DaoOperationException("Product with id = " + id + " does not exist");
        }
    }

    private Product fetchProductFromResultSet(ResultSet rs) throws SQLException {
        Product p = new Product();
        p.setId(rs.getLong("id"));
        p.setName(rs.getString("name"));
        p.setProducer(rs.getString("producer"));
        p.setPrice(rs.getBigDecimal("price"));
        p.setExpirationDate(rs.getDate("expiration_date").toLocalDate());
        p.setCreationTime(rs.getTimestamp("creation_time").toLocalDateTime());
        return p;
    }

    private void checkIfProductExists(Product product) {
        if (Objects.isNull(product)) {
            throw new DaoOperationException("Product is null");
        }
        if (Objects.isNull(product.getId())) {
            throw new DaoOperationException("Cannot find a product without ID");
        }
    }

    private Stream<Product> getProductStreamFromResultSet(ResultSet rs) {
        return StreamSupport.stream(new Spliterators.AbstractSpliterator<>(Long.MAX_VALUE, Spliterator.ORDERED) {
            @Override
            public boolean tryAdvance(Consumer<? super Product> action) {
                try {
                    if (rs.next()) {
                        action.accept(fetchProductFromResultSet(rs));
                        return true;
                    } else {
                        return false;
                    }
                } catch (SQLException e) {
                    throw new DaoOperationException("Error getting all products", e);
                }
            }
        }, false);
    }

}
