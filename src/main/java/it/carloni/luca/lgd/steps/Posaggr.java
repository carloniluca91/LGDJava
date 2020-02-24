package it.carloni.luca.lgd.steps;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import scala.collection.Seq;
import it.carloni.luca.lgd.common.AbstractStep;
import it.carloni.luca.lgd.schemas.PosaggrSchema;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static it.carloni.luca.lgd.common.StepUtils.*;

public class Posaggr extends AbstractStep {

    private final Logger logger = Logger.getLogger(Posaggr.class);

    public Posaggr(){

        stepInputDir = getValue("posaggr.input.dir");
        stepOutputDir =  getValue("posaggr.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String tblcompCsvPath = getValue("posaggr.tblcomp.path.csv");
        String tlbaggrCsvPath = getValue("posaggr.tlbaggr.path.csv");
        String tlbposiLoadCsvPath = getValue("posaggr.tlbposi.load.csv");
        String posaggrCsvPath = getValue("posaggr.out.csv");

        logger.debug("tlbcompCsvPath: " + tblcompCsvPath);
        logger.debug("tlbaggrCsvPath: " + tlbaggrCsvPath);
        logger.debug("tlbposiLoadCsvPath: " + tlbposiLoadCsvPath);
        logger.debug("posaggrOutputPath: " + posaggrCsvPath);

        // 19
        Dataset<Row> tblcomp = readCsvAtPathUsingSchema(tblcompCsvPath,
                fromPigSchemaToStructType(PosaggrSchema.getTblCompPigSchema()));

        // 32
        Dataset<Row> tlbaggr = readCsvAtPathUsingSchema(tlbaggrCsvPath,
                fromPigSchemaToStructType(PosaggrSchema.getTlbaggrPigSchema()));

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
        Dataset<Row> tlbposiLoad = readCsvAtPathUsingSchema(tlbposiLoadCsvPath,
                fromPigSchemaToStructType(PosaggrSchema.getTlbposiLoadPigSchema()));

        // 116
        Dataset<Row> tlbposi = tlbposiLoad.select(tlbposiLoad.col("dt_riferimento"), tlbposiLoad.col("cd_istituto"),
                tlbposiLoad.col("ndg"), tlbposiLoad.col("c_key"), tlbposiLoad.col("cod_fiscale"),
                tlbposiLoad.col("ndg_gruppo"),
                replaceAndConvertToDouble(tlbposiLoad, "bo_acco", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "bo_util", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "tot_add_sosp", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "tot_val_intr", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "ca_acco", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "ca_util", ",", "."),
                tlbposiLoad.col("fl_incaglio"), tlbposiLoad.col("fl_soff"), tlbposiLoad.col("fl_inc_ogg"),
                tlbposiLoad.col("fl_ristr"), tlbposiLoad.col("fl_pd_90"), tlbposiLoad.col("fl_pd_180"),
                replaceAndConvertToDouble(tlbposiLoad, "util_cassa", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "fido_op_cassa", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "utilizzo_titoli", ",", "."),
                replaceAndConvertToDouble(tlbposiLoad, "esposizione_titoli", ",", "."));

        // 140

        // 147
        // JOIN tblcomp_tlbaggr BY (dt_riferimento,cd_istituto,ndg), tlbposi BY (dt_riferimento,cd_istituto,ndg);
        Seq<String> joinColumnSeq = toScalaStringSeq((Arrays.asList("dt_riferimento", "cd_istituto", "ndg")));

        List<String> tblcompTlbaggrSelectColumnNames = Arrays.asList(
                "dt_riferimento", "cd_istituto", "ndg", "c_key_aggr",
                "tipo_segmne_aggr", "segmento", "tp_ndg");
        List<Column> tblcompTlbaggrTlbposiSelectColumnList = selectDfColumns(tlbcompTlbaggr, tblcompTlbaggrSelectColumnNames);

        // TRIM(tblcomp_tlbaggr::tp_ndg) as tp_ndg
        // selectColumnList.add(functions.trim(tlbcompTlbaggr.col("tp_ndg")).as("tp_ndg"));

        List<String> tlbposiSelectColumnNames = Arrays.asList(
                "bo_acco", "bo_util", "tot_add_sosp", "tot_val_intr", "ca_acco", "ca_util",
                "util_cassa", "fido_op_cassa", "utilizzo_titoli", "esposizione_titoli");
        tblcompTlbaggrTlbposiSelectColumnList.addAll(selectDfColumns(tlbposi, tlbposiSelectColumnNames));
        Seq<Column> selectColumnSeq = toScalaColSeq(tblcompTlbaggrTlbposiSelectColumnList);

        Dataset<Row> tblcompTlbaggrTlbposi = tlbcompTlbaggr.join(tlbposi, joinColumnSeq, "inner")
                .select(selectColumnSeq);

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

        Map<String, String> sumWindowColumns = new LinkedHashMap<String, String>(){{

            put("bo_acco", "accordato_bo");
            put("bo_util", "utilizzato_bo");
            put("tot_add_sosp", "tot_add_sosp");
            put("tot_val_intr", "tot_val_intr_ps");
            put("ca_acco", "accordato_ca");
            put("ca_util", "utilizzato_ca");
            put("util_cassa", "util_cassa");
            put("fido_op_cassa", "fido_op_cassa");
            put("utilizzo_titoli", "utilizzo_titoli");
            put("esposizione_titoli", "esposizione_titoli");
        }};

        // GROUP tblcomp_tlbaggr_tlbposi BY (dt_riferimento, cd_istituto, c_key_aggr, tipo_segmne_aggr)
        WindowSpec windowsSpec = Window.partitionBy(tblcompTlbaggrTlbposi.col("dt_riferimento"),
                tblcompTlbaggrTlbposi.col("cd_istituto"), tblcompTlbaggrTlbposi.col("c_key_aggr"),
                tblcompTlbaggrTlbposi.col("tipo_segmne_aggr"));

        /* group.dt_riferimento,
            group.cd_istituto,
            group.c_key_aggr,
            group.tipo_segmne_aggr
         */
        List<Column> tblcompTlbaggrTlbposiSelectList = selectDfColumns(tblcompTlbaggrTlbposi,
                Arrays.asList("dt_riferimento", "cd_istituto", "c_key_aggr", "tipo_segmne_aggr"));

        /*
        FLATTEN(tblcomp_tlbaggr_tlbposi.segmento) as segmento,
		FLATTEN(tblcomp_tlbaggr_tlbposi.tp_ndg) as tp_ndg
         */
        tblcompTlbaggrTlbposiSelectList.add(tblcompTlbaggrTlbposi.col("segmento"));
        tblcompTlbaggrTlbposiSelectList.add(functions.trim(tblcompTlbaggrTlbposi.col("tp_ndg")).as("tp_ndg"));
        List<Column> windowSumCols = windowSum(tblcompTlbaggrTlbposi, sumWindowColumns, windowsSpec);
        tblcompTlbaggrTlbposiSelectList.addAll(windowSumCols);

        Seq<Column> tblcompTlbaggrTlbposiSelectListselectColumnSeq = toScalaColSeq(tblcompTlbaggrTlbposiSelectList);
        Dataset<Row> posaggr = tblcompTlbaggrTlbposi.select(tblcompTlbaggrTlbposiSelectListselectColumnSeq);
        writeDatasetAsCsvAtPath(posaggr, posaggrCsvPath);
    }
}
