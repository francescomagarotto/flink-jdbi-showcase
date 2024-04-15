package org.francescomagarotto.sink;

import java.sql.Connection;
import java.sql.SQLException;
import lombok.RequiredArgsConstructor;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;

@RequiredArgsConstructor
public class JdbiBatchStatementExecutor<T> implements JdbcBatchStatementExecutor<T> {

    private PreparedBatch batch;
    private final String sql;

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.batch = Jdbi.open(connection)
                .prepareBatch(sql);
    }

    @Override
    public void addToBatch(T record) throws SQLException {
        this.batch.bindBean(record)
                .defineNamedBindings()
                .add();
    }

    @Override
    public void executeBatch() throws SQLException {
        if (batch.size() > 0) {
            batch.execute();
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        this.batch.close();
    }
}
