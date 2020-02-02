package steps.schemas;

import java.util.Map;
import java.util.stream.Stream;

public class CicliLavStep1Schema {

    public static Map<String, String> getTlbcidefPigSchema(){

        return StepSchema.fromStreamToMap(Stream.of(new String[][] {
                { "cd_isti", "chararray" },
                { "ndg_principale", "chararray" },
                { "cod_cr", "chararray" },
                { "dt_inizio_ciclo", "int" },
                { "dt_ingresso_status", "int" },
                { "status_ingresso", "chararray" },
                { "dt_uscita_status", "chararray" },
                { "status_uscita", "chararray" },
                { "dt_fine_ciclo", "chararray" },
                { "indi_pastdue", "chararray" },
                { "indi_impr_priv", "chararray" }}));
    }

    public static Map<String, String> getTlbcraccLoadPigSchema(){

        return StepSchema.fromStreamToMap(Stream.of(new String[][] {
                {"data_rif", "int" },
                {"cd_isti", "chararray" },
                {"ndg", "chararray" },
                {"cod_raccordo", "chararray" },
                {"data_val", "int" }}));
    }
}
