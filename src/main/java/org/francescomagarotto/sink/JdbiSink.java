package org.francescomagarotto.sink;

import java.io.IOException;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.OutputFormat.InitializationContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class JdbiSink<T> extends RichSinkFunction<T>
        implements CheckpointedFunction {

    private final JdbiOutputFormat<T> outputFormat;

    public JdbiSink(JdbiOutputFormat<T> outputFormat) {
        this.outputFormat = outputFormat;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext ctx = getRuntimeContext();
        outputFormat.setRuntimeContext(ctx);
        //outputFormat.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
        outputFormat.open(new InitializationContext() {
            @Override
            public int getNumTasks() {
                return ctx.getNumberOfParallelSubtasks();
            }

            @Override
            public int getTaskNumber() {
                return 1;
            }

            @Override
            public int getAttemptNumber() {
                return ctx.getAttemptNumber();
            }
        });
    }

    @Override
    public void invoke(T value, Context context) throws IOException {
        outputFormat.writeRecord(value);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        outputFormat.flush();
    }

    @Override
    public void close() {
        outputFormat.close();
    }
}