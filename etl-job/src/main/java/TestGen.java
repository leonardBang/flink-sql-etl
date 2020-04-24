//
//public final class WatermarkGenerator$0
//        extends org.apache.flink.table.runtime.generated.WatermarkGenerator {
//
//    private transient org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions$JavaFunc5 function_org$apache$flink$table$planner$runtime$utils$JavaUserDefinedScalarFunctions$JavaFunc5$ac4516f46aafeff3fbc8ae56b8d9fd58;
//    private transient org.apache.flink.table.dataformat.DataFormatConverters.TimestampConverter converter$5;
//
//    public WatermarkGenerator$0(Object[] references) throws Exception {
//        function_org$apache$flink$table$planner$runtime$utils$JavaUserDefinedScalarFunctions$JavaFunc5$ac4516f46aafeff3fbc8ae56b8d9fd58 = (((org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions$JavaFunc5) references[0]));
//        converter$5 = (((org.apache.flink.table.dataformat.DataFormatConverters.TimestampConverter) references[1]));
//    }
//
//    @Override
//    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
//
//        function_org$apache$flink$table$planner$runtime$utils$JavaUserDefinedScalarFunctions$JavaFunc5$ac4516f46aafeff3fbc8ae56b8d9fd58.open(new org.apache.flink.table.functions.FunctionContext(getRuntimeContext()));
//
//    }
//
//    @Override
//    public Long currentWatermark(org.apache.flink.table.dataformat.BaseRow row) throws Exception {
//
//        org.apache.flink.table.dataformat.SqlTimestamp field$1;
//        boolean isNull$1;
//        int field$2;
//        boolean isNull$2;
//        org.apache.flink.table.dataformat.SqlTimestamp result$3;
//        org.apache.flink.table.dataformat.SqlTimestamp result$6;
//        boolean isNull$6;
//        isNull$1 = row.isNullAt(0);
//        field$1 = null;
//        if (!isNull$1) {
//            field$1 = row.getTimestamp(0, 3);
//        }
//        isNull$2 = row.isNullAt(1);
//        field$2 = -1;
//        if (!isNull$2) {
//            field$2 = row.getInt(1);
//        }
//
//
//
//
//
//        java.sql.Timestamp javaResult$4 = (java.sql.Timestamp) function_org$apache$flink$table$planner$runtime$utils$JavaUserDefinedScalarFunctions$JavaFunc5$ac4516f46aafeff3fbc8ae56b8d9fd58.eval(isNull$1 ? null : ((org.apache.flink.table.dataformat.SqlTimestamp) field$1), isNull$2 ? null : ((java.lang.Integer) field$2));
//        result$3 = javaResult$4 == null ? null : ((org.apache.flink.table.dataformat.SqlTimestamp) converter$5.toInternal((java.sql.Timestamp) javaResult$4));
//
//
//        isNull$6 = result$3 == null;
//        result$6 = null;
//        if (!isNull$6) {
//            result$6 = result$3;
//        }
//
//        if (isNull$6) {
//            return null;
//        } else {
//            return result$6.getMillisecond();
//        }
//    }
//
//    @Override
//    public void close() throws Exception {
//
//        function_org$apache$flink$table$planner$runtime$utils$JavaUserDefinedScalarFunctions$JavaFunc5$ac4516f46aafeff3fbc8ae56b8d9fd58.close();
//
//    }
//}
