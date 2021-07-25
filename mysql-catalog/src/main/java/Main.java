import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class Main {
    public static void main(String[] args) {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        tEnv.executeSql("CREATE CATALOG mypg1 WITH(\n"
                + " 'type' = 'jdbc',\n"
                + " 'default-database' = 'test',\n"
                + " 'username' = 'root',\n"
                + " 'password' = '123456',\n"
                + "  'base-url' = 'jdbc:mysql://localhost:13306'\n"
                + ")");
        tEnv.executeSql("use catalog mypg1");
        tEnv.executeSql("use test");
        tEnv.executeSql("show current catalog").print();
        tEnv.executeSql("show current database").print();
        tEnv.executeSql("show tables").print();
        //tEnv.executeSql("desc t_alltypes")
        tEnv.executeSql("select * from t_alltypes")
                .print();
    }
}
