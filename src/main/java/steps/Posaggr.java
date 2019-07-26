package steps;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.nio.file.Paths;
import java.util.*;

public class Posaggr extends AbstractStep {

    Posaggr(String[] args){}

    @Override
    public void run() {

        // retrieve csv_format, input data directory and file name from configuration.properties file
        String csvFormat = getProperty("csv_format");
        String posaggrInputDir = getProperty("POSAGGR_INPUT_DIR");
        String tblcompCsvPath = getProperty("TBLCOMP_PATH_CSV");

        logger.info("csvFormat: " + csvFormat);
        logger.info("poaggrInputDir: " + posaggrInputDir);
        logger.info("tlbcompCsvPath: " + tblcompCsvPath);

        // 19
        List<String> tblcompColNames = Arrays.asList("dt_riferimento", "c_key", "tipo_segmne", "cd_istituto", "ndg");
        StructType tblcompSchema = getDfSchema(tblcompColNames);
        Dataset<Row> tblcomp = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(tblcompSchema).csv(
                Paths.get(posaggrInputDir, tblcompCsvPath).toString());

        // 27

        // 32
        String tlbaggrCsvPath = getProperty("TLBAGGR_PATH_CSV");
        logger.info("tlbaggrCsvPath: " + tlbaggrCsvPath);

        List<String> tlbaggrColNames = Arrays.asList("dt_riferimento", "c_key_aggr", "ndg_gruppo", "cod_fiscale",
                "tipo_segmne_aggr", "segmento", "tipo_motore", "cd_istituto", "ndg", "rae", "sae", "tp_ndg", "prov_segm",
                "fonte_segmento", "utilizzo_cr", "accordato_cr", "databil", "strutbil", "fatturbil", "attivobil",
                "codimp_cebi", "tot_acco_agr", "tot_util_agr", "n058_int_vig");
        StructType tlbaggrSchema = getDfSchema(tlbaggrColNames);
        Dataset<Row> tlbaggr = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(tlbaggrSchema).csv(
                Paths.get(posaggrInputDir, tlbaggrCsvPath).toString());

        // 59

        // 69
        // JOIN tlbaggr BY (dt_riferimento,c_key_aggr,tipo_segmne_aggr,cd_istituto), tblcomp BY ( dt_riferimento, c_key, tipo_segmne, cd_istituto);
        Column joinCondition = tlbaggr.col("dt_riferimento").equalTo(tblcomp.col("dt_riferimento"))
                .and(tlbaggr.col("c_key_aggr").equalTo(tblcomp.col("c_key")))
                .and(tlbaggr.col("tipo_segmne_aggr").equalTo(tblcomp.col("tipo_segmne")))
                .and(tlbaggr.col("cd_istituto").equalTo(tblcomp.col("cd_istituto")));

        Dataset<Row> tlbcompTlbaggr = tlbaggr.join(tblcomp, joinCondition, "inner")
                .select(tlbaggr.col("dt_riferimento"), tblcomp.col("cd_istituto"), tblcomp.col("ndg"),
                        tlbaggr.col("c_key_aggr"), tlbaggr.col("tipo_segmne_aggr"), tlbaggr.col("segmento"),
                        tlbaggr.col("tp_ndg"));
        // 79

        // 89

        String tlbposiLoadCsvPath = getProperty("TLBPOSI_LOAD_CSV");
        logger.info("tlbposiLoadCsvPath: " + tlbposiLoadCsvPath);

        List<String> tlbposiLoadColNames = Arrays.asList("dt_riferimento", "cd_istituto", "ndg", "c_key", "cod_fiscale",
                "ndg_gruppo", "bo_acco", "bo_util", "tot_add_sosp", "tot_val_intr", "ca_acco", "ca_util", "fl_incaglio",
                "fl_soff", "fl_inc_ogg", "fl_ristr", "fl_pd_90", "fl_pd_180", "util_cassa", "fido_op_cassa", "utilizzo_titoli",
                "esposizione_titoli");
        StructType tlbposiLoadSchema = getDfSchema(tlbposiLoadColNames);
        Dataset<Row> tlbposiLoad = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(tlbposiLoadSchema).csv(
                Paths.get(posaggrInputDir, tlbposiLoadCsvPath).toString());

        // 114

        // 116

        Dataset<Row> tlbposi = tlbposiLoad.select(functions.col("dt_riferimento"), functions.col("cd_istituto"),
                functions.col("ndg"), functions.col("c_key"), functions.col("cod_fiscale"), functions.col("ndg_gruppo"),
                replaceAndConvertToDouble(tlbposiLoad, "bo_acco", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "bo_util", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "tot_add_sosp", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "tot_val_intr", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "ca_acco", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "ca_util", ",", "."),
                functions.col("fl_incaglio"), functions.col("fl_soff"), functions.col("fl_inc_ogg"),
                functions.col("fl_ristr"), functions.col("fl_pd_90"), functions.col("fl_pd_180"),
                replaceAndConvertToDouble(tlbposiLoad, "util_cassa", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "fido_op_cassa", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "utilizzo_titoli", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "esposizione_titoli", ",", "."));

        // 140

        // 147
        // JOIN tblcomp_tlbaggr BY (dt_riferimento,cd_istituto,ndg), tlbposi BY (dt_riferimento,cd_istituto,ndg);
        List<String> joinColumnList = Arrays.asList("dt_riferimento", "cd_istituto", "ndg");
        Seq<String> joinColumnSeq = JavaConverters.asScalaIteratorConverter(joinColumnList.iterator()).asScala().toSeq();

        List<String> selectColumnNames = Arrays.asList(
                "dt_riferimento", "cd_istituto", "ndg", "c_key_aggr", "tipo_segmne_aggr", "segmento", "tp_ndg");
        List<Column> selectColumnList = selectDfColumns(tlbcompTlbaggr, selectColumnNames);

        // TRIM(tblcomp_tlbaggr::tp_ndg) as tp_ndg
        // selectColumnList.add(functions.trim(tlbcompTlbaggr.col("tp_ndg")).as("tp_ndg"));

        selectColumnNames = Arrays.asList("bo_acco", "bo_util", "tot_add_sosp", "tot_val_intr", "ca_acco", "ca_util",
                "util_cassa", "fido_op_cassa", "utilizzo_titoli", "esposizione_titoli");
        selectColumnList.addAll(selectDfColumns(tlbposi, selectColumnNames));
        Seq<Column> selectColumnSeq = JavaConverters.asScalaIteratorConverter(selectColumnList.iterator()).asScala().toSeq();

        Dataset<Row> tblcompTlbaggrTlbposi = tlbcompTlbaggr.join(tlbposi, joinColumnSeq, "inner").select(selectColumnSeq);

        // 167
        /*
        DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.bo_acco)) as accordato_bo
              ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.bo_util)) as utilizzato_bo
              ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.tot_add_sosp)) as tot_add_sosp
              ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.tot_val_intr)) as tot_val_intr_ps
              ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.ca_acco)) as accordato_ca
              ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.ca_util)) as utilizzato_ca
              ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.util_cassa)) as util_cassa
			  ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.fido_op_cassa)) as fido_op_cassa
			  ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.utilizzo_titoli)) as utilizzo_titoli
			  ,DoubleConverter(SUM(tblcomp_tlbaggr_tlbposi.esposizione_titoli)) as esposizione_titoli
         */

        Map<String, String> sumWindowColumns = new HashMap<>();
        sumWindowColumns.put("bo_acco", "accordato_bo");
        sumWindowColumns.put("bo_util", "utilizzato_bo");
        sumWindowColumns.put("tot_add_sosp", "tot_add_sosp");
        sumWindowColumns.put("tot_val_intr", "tot_val_intr_ps");
        sumWindowColumns.put("ca_acco", "accordato_ca");
        sumWindowColumns.put("ca_util", "utilizzato_ca");
        sumWindowColumns.put("util_cassa", "util_cassa");
        sumWindowColumns.put("fido_op_cassa", "fido_op_cassa");
        sumWindowColumns.put("utilizzo_titoli", "utilizzo_titoli");
        sumWindowColumns.put("esposizione_titoli", "esposizione_titoli");

        // GROUP tblcomp_tlbaggr_tlbposi BY (dt_riferimento, cd_istituto, c_key_aggr, tipo_segmne_aggr)
        WindowSpec w = Window.partitionBy(tblcompTlbaggrTlbposi.col("dt_riferimento"),
                tblcompTlbaggrTlbposi.col("cd_istituto"), tblcompTlbaggrTlbposi.col("c_key_aggr"),
                        tblcompTlbaggrTlbposi.col("tipo_segmne_aggr"));

        /* group.dt_riferimento,
            group.cd_istituto,
            group.c_key_aggr,
            group.tipo_segmne_aggr
         */
        List<Column> tblcompTlbaggrTlbposiSelectList = selectDfColumns(tblcompTlbaggrTlbposi, Arrays.asList("dt_riferimento",
                "cd_istituto", "c_key_aggr", "tipo_segmne_aggr"));

        /*
        FLATTEN(tblcomp_tlbaggr_tlbposi.segmento) as segmento,
		FLATTEN(tblcomp_tlbaggr_tlbposi.tp_ndg) as tp_ndg
         */
        tblcompTlbaggrTlbposiSelectList.add(tblcompTlbaggrTlbposi.col("segmento"));
        tblcompTlbaggrTlbposiSelectList.add(functions.trim(tblcompTlbaggrTlbposi.col("tp_ndg")).as("tp_ndg"));
        List<Column> windowSumCols = windowSum(tblcompTlbaggrTlbposi, sumWindowColumns, w);
        tblcompTlbaggrTlbposiSelectList.addAll(windowSumCols);

        Seq<Column> tblcompTlbaggrTlbposiSelectListselectColumnSeq =
                JavaConverters.asScalaIteratorConverter(selectColumnList.iterator()).asScala().toSeq();
        Dataset<Row> posaggr = tblcompTlbaggrTlbposi.select(tblcompTlbaggrTlbposiSelectListselectColumnSeq);

        String posaggrOutputDir = getProperty("POSAGGR_OUTPUT_DIR");
        String posaggrCsvPath = getProperty("POSAGGR_CSV");
        logger.info("posaggrOutputDir: " + posaggrOutputDir);
        logger.info("posaggrOutputPath: " + posaggrCsvPath);

        posaggr.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(
                Paths.get(posaggrOutputDir, posaggrCsvPath).toString());
    }

    private Column replaceAndConvertToDouble(Dataset<Row> df, String columnName, String oldString, String newString){

        return functions.regexp_replace(df.col(columnName), oldString, newString)
                .cast(DataTypes.DoubleType).as(columnName);
    }

    private List<Column> windowSum(Dataset<Row> df, Map <String,String> columnMap, WindowSpec w){

        List<Column> columnList = new ArrayList<>();
        Set<Map.Entry<String, String>> entryList = columnMap.entrySet();

        for (Map.Entry<String, String> entry: entryList){

            columnList.add(functions.sum(df.col(entry.getKey())).over(w).alias(entry.getValue()));
        }
        return columnList;
    }
}
