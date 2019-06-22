package steps;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;

import java.util.logging.Logger;


public class CicliNdgMonthly extends AbstractStep{

    private final Options options = new Options();
    private final Logger logger = Logger.getLogger(CicliNdgMonthly.class.getName());

    public CicliNdgMonthly(String[] parameters) {

        super(parameters);
        Option periodo = new Option("p", "periodo", true, "parametro periodo");
        periodo.setRequired(true);
        options.addOption(periodo);
    }

    public void run(){

        String tlbciclilavPath = getProperty("TLBCICLILAV_PATH");
        String tlbcompPath = getProperty("TLBCOMP_PATH");
        String tlbaggrPath = getProperty("TLBAGGR_PATH");

        // dataframe ciclilav1
        // 24
        StructField[] ciclilav1Columns = new StructField[]{

                new StructField("cd_isti", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ndg_principale", DataTypes.StringType, true, Metadata.empty()),
                new StructField("dt_inizio_ciclo", DataTypes.StringType, true, Metadata.empty()),
                new StructField("dt_fine_ciclo", DataTypes.StringType, true, Metadata.empty()),
                new StructField("datainiziopd", DataTypes.StringType, true, Metadata.empty()),
                new StructField("datainizioristrutt", DataTypes.StringType, true, Metadata.empty()),
                new StructField("datainizioinc", DataTypes.StringType, true, Metadata.empty()),
                new StructField("datainiziosoff", DataTypes.StringType, true, Metadata.empty()),
                new StructField("progr", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("cd_isti_coll", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ndg_coll", DataTypes.StringType, true, Metadata.empty()),
                new StructField("dt_riferimento_udct", DataTypes.StringType, true, Metadata.empty()),
                new StructField("dt_riferimento_urda", DataTypes.StringType, true, Metadata.empty()),
                new StructField("util_cassa", DataTypes.StringType, true, Metadata.empty()),
                new StructField("imp_accordato", DataTypes.StringType, true, Metadata.empty()),
                new StructField("tot_add_sosp", DataTypes.StringType, true, Metadata.empty()),
                new StructField("cd_collegamento", DataTypes.StringType, true, Metadata.empty()),
                new StructField("status_ndg", DataTypes.StringType, true, Metadata.empty()),
                new StructField("posiz_soff_inc", DataTypes.StringType, true, Metadata.empty()),
                new StructField("cd_rap_ristr", DataTypes.StringType, true, Metadata.empty()),
                new StructField("tp_cli_scad_scf", DataTypes.StringType, true, Metadata.empty())};

        StructType ciclilav1Schema = new StructType(ciclilav1Columns);
        Dataset<Row> ciclilav1 = sparkSession.read().format("com.databricks.spark.csv")
                .option("delimiter", ",").schema(ciclilav1Schema).csv(tlbciclilavPath);

        // column expression for converting "dt_inizio_ciclo" to timestamp
        Column colDtInizioCiclo = functions.unix_timestamp(functions.substring(ciclilav1.col("dt_inizio_ciclo")
                .cast(DataTypes.StringType), 0, 7), "yyyy-MM");

        // column expression for converting parameter "periodo" to timestamp
        Column colPeriodo = functions.unix_timestamp(functions.substring(functions.lit(
                options.getOption("periodo")), 0, 7), "yyyy-MM");

        // column expression for converting parameter "periodo" to timestamp
        Column colPeriodoCase = functions.unix_timestamp(functions.when(functions.substring(functions.lit(options.getOption("periodo")), 7, 7)
                        .isNotNull(), functions.substring(functions.lit(options.getOption("periodo")), 7, 7))
                .otherwise(colPeriodo), "yyyy-MM");

        Dataset<Row> ciclilav = ciclilav1.filter(colDtInizioCiclo.gt(colPeriodo).and(colDtInizioCiclo.lt(colPeriodoCase)));

        // 56


        // dataframe tblcomp
        // 61
        StructField[] tblcompColumns = new StructField[]{
                new StructField("dt_riferimento", DataTypes.StringType, true, Metadata.empty()),
                new StructField("c_key", DataTypes.StringType, true, Metadata.empty()),
                new StructField("tipo_segmne", DataTypes.StringType, true, Metadata.empty()),
                new StructField("cd_istituto", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ndg", DataTypes.StringType, true, Metadata.empty())};

        StructType tblcompSchema = new StructType(tblcompColumns);
        Dataset<Row> tblcomp = sparkSession.read().format("com.databricks.spark.csv")
                .option("delimiter", ",").schema(tblcompSchema).csv(tlbcompPath);

        // 69

        // 80
        Dataset<Row> tlbcidefTblcompJoin = ciclilav.join(tblcomp, ciclilav.col("cd_isti_coll").equalTo(
                tblcomp.col("cd_istituto")).and(ciclilav.col("ndg_coll").equalTo(tblcomp.col("ndg")))
                .and(ciclilav.col("dt_inizio_ciclo").equalTo(tblcomp.col("dt_riferimento"))))
                .select(ciclilav.col("cd_isti"), ciclilav.col("ndg_principale"),
                        ciclilav.col("dt_inizio_ciclo"), ciclilav.col("progr"),
                        ciclilav.col("cd_isti_coll"), ciclilav.col("ndg_coll"),
                        tblcomp.col("c_key"), tblcomp.col("tipo_segmne"));

        // 92


        // 97
        WindowSpec w = Window.orderBy(functions.col("progr").asc(), functions.col("c_key").asc(),
                functions.col("tipo_segmne").asc());

        Dataset<Row> tlbcidefTblcomp = tlbcidefTblcompJoin.select(functions.col("cd_isti"),
                functions.col("dt_inizio_ciclo"), functions.col("ndg_principale")).distinct()
                .withColumn("c_key", functions.first(functions.col("c_key")).over(w))
                .withColumn("tipo_segmne", functions.first(functions.col("tipo_segmne")).over(w))
                .withColumn("cd_isti_coll", functions.first(functions.col("cd_isti_coll")).over(w))
                .withColumn("ndg_coll", functions.first(functions.col("ndg_coll")).over(w));

        // 115

        // dataframe tlbaggr
        // 120
        StructField[] tlbaggrColumns = new StructField[]{
                new StructField("dt_riferimento", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("c_key_aggr", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ndg_gruppo", DataTypes.StringType, true, Metadata.empty()),
                new StructField("cod_fiscale", DataTypes.StringType, true, Metadata.empty()),
                new StructField("tipo_segmne_aggr", DataTypes.StringType, true, Metadata.empty()),
                new StructField("segmento", DataTypes.StringType, true, Metadata.empty()),
                new StructField("tipo_motore", DataTypes.StringType, true, Metadata.empty()),
                new StructField("cd_istituto", DataTypes.StringType, true, Metadata.empty()),
                new StructField("ndg", DataTypes.StringType, true, Metadata.empty()),
                new StructField("rae", DataTypes.StringType, true, Metadata.empty()),
                new StructField("sae", DataTypes.StringType, true, Metadata.empty()),
                new StructField("tp_ndg", DataTypes.StringType, true, Metadata.empty()),
                new StructField("prov_segm", DataTypes.StringType, true, Metadata.empty()),
                new StructField("fonte_segmento", DataTypes.StringType, true, Metadata.empty()),
                new StructField("utilizzo_cr", DataTypes.StringType, true, Metadata.empty()),
                new StructField("accordato_cr", DataTypes.StringType, true, Metadata.empty()),
                new StructField("databil", DataTypes.StringType, true, Metadata.empty()),
                new StructField("strutbil", DataTypes.StringType, true, Metadata.empty()),
                new StructField("fatturbil", DataTypes.StringType, true, Metadata.empty()),
                new StructField("attivobil", DataTypes.StringType, true, Metadata.empty()),
                new StructField("codimp_cebi", DataTypes.StringType, true, Metadata.empty()),
                new StructField("tot_acco_agr", DataTypes.StringType, true, Metadata.empty()),
                new StructField("tot_util_agr", DataTypes.StringType, true, Metadata.empty()),
                new StructField("n058_int_vig", DataTypes.StringType, true, Metadata.empty())
        };

        StructType tlbaggrSchema = new StructType(tlbaggrColumns);
        Dataset<Row> tlbaggr = sparkSession.read().format("com.databricks.spark.csv")
                .option("delimiter", ",").schema(tlbaggrSchema).csv(tlbaggrPath)
                .select(functions.col("dt_riferimento"), functions.col("c_key_aggr"),
                        functions.col("cd_istituto"), functions.col("tipo_segmne_aggr"),
                        functions.col("sae").as("sae_segm"), functions.col("rae").as("rae_segm"),
                        functions.col("segmento"), functions.col("tp_ndg"),
                        functions.col("prov_segm").as("provincia_segm"), functions.col("databil").as("databilseg"),
                        functions.col("strutbil").as("strbilseg"), functions.col("attivobil").as("attivobilseg"),
                        functions.col("fatturbil").as("fatturbilseg"), functions.col("cod_fiscale"));

        // 165

        // dataframe dominisegmento
        // 168
        String confDirPath = getProperty("CONF_DIR");
        StructField[] dominiSegmentoColumns = new StructField[]{
                new StructField("segmento", DataTypes.StringType, true, Metadata.empty()),
                new StructField("segmento_desc", DataTypes.StringType, true, Metadata.empty())
        };

        StructType dominiSegmentoSchema = new StructType(dominiSegmentoColumns);
        Dataset<Row> dominiSegmento = sparkSession.read().format("com.databricks.spark.csv").option("delimiter", ",")
                .schema(dominiSegmentoSchema).csv(confDirPath);

        // 172

        // 173
        Dataset<Row> tlbaggrSegmentoDominioJoin = tlbaggr.join(dominiSegmento,
                tlbaggr.col("segmento").equalTo(dominiSegmento.col("segmento")));

        // skip definition of dataframe tlbaggr_filtered
        // 176

        // 185
        Dataset<Row> tblcompTblaggr = tlbcidefTblcomp.join(tlbaggrSegmentoDominioJoin,
                tlbcidefTblcompJoin.col("dt_inizio_ciclo").equalTo(tlbaggrSegmentoDominioJoin.col("dt_riferimento"))
                        .and(tlbcidefTblcompJoin.col("c_key").equalTo(tlbaggrSegmentoDominioJoin.col("c_key_aggr")))
                .and(tlbcidefTblcompJoin.col("tipo_segmne").equalTo(tlbaggrSegmentoDominioJoin.col("tipo_segmne_aggr"))))
                .select(tlbcidefTblcompJoin.col("cd_isti"), tlbcidefTblcompJoin.col("ndg_principale"),
                        tlbcidefTblcompJoin.col("dt_inizio_ciclo"), tlbcidefTblcompJoin.col("tipo_segmne"),
                        tlbcidefTblcompJoin.col("c_key"), tlbcidefTblcompJoin.col("cd_isti_coll"),
                        tlbcidefTblcompJoin.col("ndg_coll"),
                        tlbaggrSegmentoDominioJoin.col("sae_segm"), tlbaggrSegmentoDominioJoin.col("rae_segm"),
                        tlbaggrSegmentoDominioJoin.col("segmento"), tlbaggrSegmentoDominioJoin.col("tp_ndg"),
                        tlbaggrSegmentoDominioJoin.col("provincia_segm"), tlbaggrSegmentoDominioJoin.col("databilseg"),
                        tlbaggrSegmentoDominioJoin.col("strbilseg"), tlbaggrSegmentoDominioJoin.col("attivobilseg"),
                        tlbaggrSegmentoDominioJoin.col("fatturbilseg"), tlbaggrSegmentoDominioJoin.col("cod_fiscale"),
                        tlbaggrSegmentoDominioJoin.col("segmento_desc"));

        // 206

        // 271
    }

}
