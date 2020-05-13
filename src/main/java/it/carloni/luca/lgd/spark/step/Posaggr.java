package it.carloni.luca.lgd.spark.step;

import it.carloni.luca.lgd.parameter.step.EmptyValue;
import it.carloni.luca.lgd.schema.PosaggrSchema;
import it.carloni.luca.lgd.spark.common.AbstractStep;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;

import java.util.Arrays;

import static it.carloni.luca.lgd.spark.utils.StepUtils.toScalaSeq;

public class Posaggr extends AbstractStep<EmptyValue> {

    private final Logger logger = Logger.getLogger(getClass());

    @Override
    public void run(EmptyValue emptyValue) {

        String tblcompCsvPath = getValue("posaggr.tblcomp.path.csv");
        String tlbaggrCsvPath = getValue("posaggr.tlbaggr.path.csv");
        String tlbposiLoadCsvPath = getValue("posaggr.tlbposi.load.csv");
        String posaggrCsvPath = getValue("posaggr.out.csv");

        logger.info("posaggr.tblcomp.path.csv: " + tblcompCsvPath);
        logger.info("posaggr.tlbaggr.path.csv: " + tlbaggrCsvPath);
        logger.info("posaggr.tlbposi.load.csv: " + tlbposiLoadCsvPath);
        logger.info("posaggr.out.csv: " + posaggrCsvPath);

        // 19
        Dataset<Row> tblcomp = readCsvAtPathUsingSchema(tblcompCsvPath, PosaggrSchema.getTblCompPigSchema());

        // 32
        Dataset<Row> tlbaggr = readCsvAtPathUsingSchema(tlbaggrCsvPath, PosaggrSchema.getTlbaggrPigSchema());

        // 69
        // JOIN tlbaggr BY (dt_riferimento,c_key_aggr,tipo_segmne_aggr,cd_istituto), tblcomp BY ( dt_riferimento, c_key, tipo_segmne, cd_istituto);
        Column joinCondition = tlbaggr.col("dt_riferimento").equalTo(tblcomp.col("dt_riferimento"))
                .and(tlbaggr.col("c_key_aggr").equalTo(tblcomp.col("c_key")))
                .and(tlbaggr.col("tipo_segmne_aggr").equalTo(tblcomp.col("tipo_segmne")))
                .and(tlbaggr.col("cd_istituto").equalTo(tblcomp.col("cd_istituto")));

        Dataset<Row> tlbcompTlbaggr = tlbaggr.join(tblcomp, joinCondition)
                .select(tlbaggr.col("dt_riferimento"), tblcomp.col("cd_istituto"), tblcomp.col("ndg"),
                        tlbaggr.col("c_key_aggr"), tlbaggr.col("tipo_segmne_aggr"), tlbaggr.col("segmento"),
                        tlbaggr.col("tp_ndg"));
        // 79

        // 89
        Dataset<Row> tlbposiLoad = readCsvAtPathUsingSchema(tlbposiLoadCsvPath, PosaggrSchema.getTlbposiLoadPigSchema());

        // 116
        Dataset<Row> tlbposi = tlbposiLoad.select(tlbposiLoad.col("dt_riferimento"), tlbposiLoad.col("cd_istituto"),
                tlbposiLoad.col("ndg"), tlbposiLoad.col("c_key"), tlbposiLoad.col("cod_fiscale"),
                tlbposiLoad.col("ndg_gruppo"),
                replaceAndToDouble(tlbposiLoad, "bo_acco"),
                replaceAndToDouble(tlbposiLoad, "bo_util"),
                replaceAndToDouble(tlbposiLoad, "tot_add_sosp"),
                replaceAndToDouble(tlbposiLoad, "tot_val_intr"),
                replaceAndToDouble(tlbposiLoad, "ca_acco"),
                replaceAndToDouble(tlbposiLoad, "ca_util"),
                tlbposiLoad.col("fl_incaglio"), tlbposiLoad.col("fl_soff"), tlbposiLoad.col("fl_inc_ogg"),
                tlbposiLoad.col("fl_ristr"), tlbposiLoad.col("fl_pd_90"), tlbposiLoad.col("fl_pd_180"),
                replaceAndToDouble(tlbposiLoad, "util_cassa"),
                replaceAndToDouble(tlbposiLoad, "fido_op_cassa"),
                replaceAndToDouble(tlbposiLoad, "utilizzo_titoli"),
                replaceAndToDouble(tlbposiLoad, "esposizione_titoli"));

        // 140

        // 147
        // JOIN tblcomp_tlbaggr BY (dt_riferimento,cd_istituto,ndg), tlbposi BY (dt_riferimento,cd_istituto,ndg);
        Seq<String> joinColumnSeq = toScalaSeq((Arrays.asList("dt_riferimento", "cd_istituto", "ndg")));

        Dataset<Row> tblcompTlbaggrTlbposi = tlbcompTlbaggr.join(tlbposi, joinColumnSeq, "inner")
                .select(tlbcompTlbaggr.col("dt_riferimento"), tlbcompTlbaggr.col("cd_istituto"),
                        tlbcompTlbaggr.col("ndg"), tlbcompTlbaggr.col("c_key_aggr"), tlbcompTlbaggr.col("tipo_segmne_aggr"),
                        tlbcompTlbaggr.col("segmento"), tlbcompTlbaggr.col("tp_ndg"),
                        functions.trim(tlbcompTlbaggr.col("tp_ndg")).as("tp_ndg"), tlbposi.col("bo_acco"),
                        tlbposi.col("bo_util"), tlbposi.col("tot_add_sosp"), tlbposi.col("tot_val_intr"),
                        tlbposi.col("ca_acco"), tlbposi.col("ca_util"), tlbposi.col("util_cassa"),
                        tlbposi.col("fido_op_cassa"), tlbposi.col("utilizzo_titoli"), tlbposi.col("esposizione_titoli"));

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

        // GROUP tblcomp_tlbaggr_tlbposi BY (dt_riferimento, cd_istituto, c_key_aggr, tipo_segmne_aggr)
        WindowSpec windowsSpec = Window.partitionBy("dt_riferimento", "cd_istituto", "c_key_aggr", "tipo_segmne_aggr");

        Dataset<Row> posaggr = tblcompTlbaggrTlbposi
                .select(functions.col("dt_riferimento"), functions.col("cd_istituto"), functions.col("c_key_aggr"),
                        functions.col("tipo_segmne_aggr"), functions.col("segmento"), functions.col("tp_ndg"),
                        sumOverWindowAndToDouble(functions.col("bo_acco"), "accordato_bo", windowsSpec),
                        sumOverWindowAndToDouble(functions.col("bo_util"), "utilizzato_bo", windowsSpec),
                        sumOverWindowAndToDouble(functions.col("tot_add_sosp"), "tot_add_sosp", windowsSpec),
                        sumOverWindowAndToDouble(functions.col("tot_val_intr"), "tot_val_intr_ps", windowsSpec),
                        sumOverWindowAndToDouble(functions.col("ca_acco"), "accordato_ca", windowsSpec),
                        sumOverWindowAndToDouble(functions.col("ca_util"), "utilizzato_ca", windowsSpec),
                        sumOverWindowAndToDouble(functions.col("util_cassa"), "util_cassa", windowsSpec),
                        sumOverWindowAndToDouble(functions.col("fido_op_cassa"), "fido_op_cassa", windowsSpec),
                        sumOverWindowAndToDouble(functions.col("utilizzo_titoli"), "utilizzo_titoli", windowsSpec),
                        sumOverWindowAndToDouble(functions.col("esposizione_titoli"), "esposizione_titoli", windowsSpec));

        writeDatasetAsCsvAtPath(posaggr, posaggrCsvPath);
    }

    private Column replaceAndToDouble(Dataset<Row> df, String columnName){

        return functions.regexp_replace(df.col(columnName), ",", ".")
                .cast(DataTypes.DoubleType).as(columnName);
    }

    private Column sumOverWindowAndToDouble(Column column, String alias, WindowSpec windowSpec) {

        return functions.sum(column).over(windowSpec).cast(DataTypes.DoubleType).as(alias);
    }
}
