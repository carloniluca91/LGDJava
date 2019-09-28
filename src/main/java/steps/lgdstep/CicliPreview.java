package steps.lgdstep;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.StructType;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class CicliPreview extends AbstractStep {

    // required parameters
    private String dataA;
    private String ufficio;

    public CicliPreview(String dataA, String ufficio){

        logger = Logger.getLogger(this.getClass().getName());

        this.dataA = dataA;
        this.ufficio = ufficio;

        stepInputDir = getProperty("cicli.preview.input.dir");
        stepOutputDir = getProperty("cicli.preview.output.dir");

        logger.info("stepInputDir: " + stepInputDir);
        logger.info("stepOutputDir: " + stepOutputDir);
        logger.info("setting dataA to " + this.dataA);
        logger.info("setting ufficio to " + this.ufficio);
    }

    public void run(){

        String csvFormat = getProperty("csv.format");
        String fposiOutdirCsv = getProperty("fposi.outdir.csv");

        logger.info("csvFormat: " + csvFormat);
        logger.info("fposiOutdirCsv: " + fposiOutdirCsv);

        // define dataset schema
        List<String> fposiColumns = Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef", "datafinedef", "datainiziopd", "datainizioinc",
                "datainizioristrutt", "datasofferenza", "totaccordatodatdef", "totutilizzdatdef", "segmento", "naturagiuridica_segm");
        StructType fposiLoadSchema = getDfSchema(fposiColumns);

        // 21
        String fposiOutdirCsvPath = Paths.get(stepInputDir, fposiOutdirCsv).toString();
        logger.info("fposiOutdirCsvPath: " + fposiOutdirCsvPath);
        Dataset<Row> fposiLoad = sparkSession.read().format(csvFormat).option("delimiter", ",").
                schema(fposiLoadSchema).csv(fposiOutdirCsvPath);

        //36

        //53
        // (naturagiuridica_segm != 'CO' AND segmento in ('01','02','03','21')?'IM': (segmento == '10'?'PR':'AL')) as segmento_calc
        Column segmentoCalcCol = functions.when(fposiLoad.col("naturagiuridica_segm").notEqual("CO")
                .and(fposiLoad.col("segmento").isin("01", "02", "03", "21")), "IM").otherwise(functions.when(
                        fposiLoad.col("segmento").equalTo("10"), "PR").otherwise("AL")).as("segmento_calc");

        Column cicloSoffCol = functions.when(fposiLoad.col("datasofferenza").isNull(), "N").otherwise("S").as("ciclo_soff");

        // define filtering column conditions ...
        Column dataInizioPdFilterCol = getDateColumnCondition(fposiLoad, "datainiziopd");
        Column dataInizioIncFilterCol = getDateColumnCondition(fposiLoad, "datainizioinc");
        Column dataInizioRistruttFilterCol = getDateColumnCondition(fposiLoad, "datainizioristrutt");
        Column dataSofferenzaFilterCol = getDateColumnCondition(fposiLoad, "datasofferenza");

        // as well as their timestamp counterparts
        Column dataInizioPdFilterTSCol = getUnixTimeStampCol(dataInizioPdFilterCol, "yyyyMMdd");
        Column dataInizioIncFilterTSCol = getUnixTimeStampCol(dataInizioIncFilterCol, "yyyyMMdd");
        Column dataInizioRistruttFilterTSCol = getUnixTimeStampCol(dataInizioRistruttFilterCol, "yyyyMMdd");
        Column dataSofferenzaFilterTSCol = getUnixTimeStampCol(dataSofferenzaFilterCol, "yyyyMMdd");

         /*
        ( datasofferenza is not null and
        (datasofferenza<(datainiziopd is null?'99999999':datainiziopd) and
         datasofferenza<(datainizioinc is null?'99999999':datainizioinc) and
         datasofferenza<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'SOFF': 'PASTDUE')
        */

        Column dataSofferenzaTSCol = getUnixTimeStampCol(fposiLoad, "datasofferenza", "yyyyMMdd");
        Column dataSofferenzaCaseWhenCol = functions.when(fposiLoad.col("datasofferenza").isNotNull()
                .and(dataSofferenzaTSCol.$less(dataInizioPdFilterTSCol))
                .and(dataSofferenzaTSCol.$less(dataInizioIncFilterTSCol))
                .and(dataSofferenzaTSCol.$less(dataInizioRistruttFilterTSCol)),
                "SOFF").otherwise("PASTDUE");

        /*
        datainizioristrutt is not null and
        (datainizioristrutt<(datainiziopd is null?'99999999':datainiziopd) and
        datainizioristrutt<(datainizioinc is null?'99999999':datainizioinc) and
        datainizioristrutt<(datasofferenza is null?'99999999':datasofferenza))? 'RISTR'
         */

        Column datainizioRistruttTSCol = getUnixTimeStampCol(fposiLoad, "datainizioristrutt", "yyyyMMdd");
        Column dataInizioRistruttCaseWhenCol = functions.when(fposiLoad.col("datainizioristrutt").isNotNull()
                .and(datainizioRistruttTSCol.$less(dataInizioPdFilterTSCol))
                .and(datainizioRistruttTSCol.$less(dataInizioIncFilterTSCol))
                .and(datainizioRistruttTSCol.$less(dataSofferenzaFilterTSCol)),
                "RISTR").otherwise(dataSofferenzaCaseWhenCol);

        /*
        ( datainizioinc is not null and
         (datainizioinc<(datainiziopd is null?'99999999':datainiziopd) and
          datainizioinc<(datasofferenza is null?'99999999':datasofferenza) and
          datainizioinc<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'INCA':
         */
        Column dataInizioIncTSCol = getUnixTimeStampCol(fposiLoad, "datainizioinc", "yyyMMdd");
        Column dataInizioIncCaseWhenCol = functions.when(fposiLoad.col("datainizioinc").isNotNull()
                .and(dataInizioIncTSCol.$less(dataInizioPdFilterTSCol))
                .and(dataInizioIncTSCol.$less(dataSofferenzaFilterTSCol))
                .and(dataInizioIncTSCol.$less(dataInizioRistruttFilterTSCol)),
                "INCA").otherwise(dataInizioRistruttCaseWhenCol);

        /*
        ( datainiziopd is not null and
         (datainiziopd<(datasofferenza is null?'99999999':datasofferenza) and
          datainiziopd<(datainizioinc is null?'99999999':datainizioinc) and
          datainiziopd<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'PASTDUE':

         */
        Column dataInizioPdTSCol = getUnixTimeStampCol(fposiLoad, "datainiziopd", "yyyyMMdd");
        Column dataInizioPdNotNullCaseWhenCol = functions.when(fposiLoad.col("datainiziopd").isNotNull()
                .and(dataInizioPdTSCol.$less(dataSofferenzaFilterTSCol))
                .and(dataInizioPdTSCol.$less(dataInizioIncFilterTSCol))
                .and(dataInizioPdTSCol.$less(dataInizioRistruttFilterTSCol)),
                "PASTDUE").otherwise(dataInizioIncCaseWhenCol);

        /*
        (datainiziopd is null and
         datainizioinc is null and
         datainizioristrutt is null and
         datasofferenza is null)?'PASTDUE':
         */

        Column statoAnagraficoCol = functions.when(fposiLoad.col("datainiziopd").isNull()
                .and(fposiLoad.col("datainizioinc").isNull())
                .and(fposiLoad.col("datainizioristrutt").isNull())
                .and(fposiLoad.col("datasofferenza").isNull()), "PASTDUE").otherwise(dataInizioPdNotNullCaseWhenCol)
                .as("stato_anagrafico");

        Column dataFineDefTSCol = getUnixTimeStampCol(fposiLoad, "datafinedef", "yyyyMMdd");
        Column flagApertoCol = functions.when(dataFineDefTSCol.$greater(functions.unix_timestamp(functions.lit(dataA), "yyyy-MM-dd")),
                "A").otherwise("C").as("flag_aperto");

        Dataset<Row> fposiBase = fposiLoad.select(functions.lit(ufficio).as("ufficio"), functions.col("codicebanca"),
                functions.lit(dataA).as("datarif"), functions.col("ndgprincipale"), functions.col("datainiziodef"),
                functions.col("datafinedef"), functions.col("datainiziopd"), functions.col("datainizioinc"),
                functions.col("datainizioristrutt"), functions.col("datasofferenza"), functions.col("totaccordatodatdef"),
                functions.col("totutilizzdatdef"), segmentoCalcCol, cicloSoffCol, statoAnagraficoCol, flagApertoCol);

        // 81

        // 83

        /*
        FLATTEN(fposi_base.ufficio)             as ufficio
        ,group.codicebanca                       as codicebanca
        ,FLATTEN(fposi_base.datarif)             as datarif
        ,group.ndgprincipale                     as ndgprincipale
        ,group.datainiziodef                     as datainiziodef
        ,FLATTEN(fposi_base.datafinedef)         as datafinedef
        ,FLATTEN(fposi_base.datainiziopd)        as datainiziopd
        ,FLATTEN(fposi_base.datainizioinc)       as datainizioinc
        ,FLATTEN(fposi_base.datainizioristrutt)  as datainizioristrutt
        ,FLATTEN(fposi_base.datasofferenza)      as datasofferenza
        ,SUM(fposi_base.totaccordatodatdef)      as totaccordatodatdef
        ,SUM(fposi_base.totutilizzdatdef)        as totutilizzdatdef
        ,FLATTEN(fposi_base.segmento_calc)       as segmento_calc
        ,FLATTEN(fposi_base.ciclo_soff)          as ciclo_soff
        ,FLATTEN(fposi_base.stato_anagrafico)    as stato_anagrafico

         */

        // ToString(ToDate(datainiziodef,'yyyyMMdd'),'yyyy-MM-dd') as datainiziodef
        Column dataInizioDefCol = castToDateCol(
                fposiBase.col("datainiziodef"), "yyyyMMdd", "yyyy-MM-dd").as("datainiziodef");

        // castToDateCol(fposiBase.col("datafinedef"), "yyyyMMdd", "yyyy-MM-dd").as("datafinedef"),
        Column dataFineDefCol = castToDateCol(
                fposiBase.col("datafinedef"), "yyyyMMdd", "yyyy-MM-dd").as("datafinedef");

        // castToDateCol(fposiBase.col("datainiziopd"), "yyyyMMdd", "yyyy-MM-dd").as("datafinedef"),
        Column dataInizioPdCol = castToDateCol(
                fposiBase.col("datainiziopd"), "yyyyMMdd", "yyyy-MM-dd").as("datainiziopd");

        // castToDateCol(fposiBase.col("datainizioinc"), "yyyyMMdd", "yyyy-MM-dd").as("datainizioinc"),
        Column dataInizioIncCol = castToDateCol(
                fposiBase.col("datainizioinc"), "yyyyMMdd", "yyyy-MM-dd").as("datainizioinc");

        // castToDateCol(fposiBase.col("datainizioristrutt"), "yyyyMMdd", "yyyy-MM-dd").as("datainizioristrutt"),
        Column dataInizioRistruttCol = castToDateCol(
                fposiBase.col("datainizioristrutt"), "yyyyMMdd", "yyyy-MM-dd").as("datainizioristrutt");

        // castToDateCol(fposiBase.col("datasofferenza"), "yyyyMMdd", "yyyy-MM-dd").as("datasofferenza"),
        Column dataSofferenzaCol = castToDateCol(
                fposiBase.col("datasofferenza"), "yyyyMMdd", "yyyy-MM-dd").as("datasofferenza");

        // define WindowSpec in order to compute aggregates on fposiBase without grouping
        WindowSpec w = Window.partitionBy("codicebanca", "ndgprincipale", "datainiziodef");

        // SUM(fposi_base.totaccordatodatdef)      as totaccordatodatdef
        // SUM(fposi_base.totutilizzdatdef)        as totutilizzdatdef
        Column totAccordatoDatDefCol = functions.sum(fposiBase.col("totaccordatodatdef")).over(w).as("totaccordatodatdef");
        Column totUtilizzDatDefCol = functions.sum(fposiBase.col("totutilizzdatdef")).over(w).as("totutilizzdatdef");

        Dataset<Row> fposiGen2 = fposiBase.select(functions.col("ufficio"), functions.col("codicebanca"),
                functions.col("datarif"), functions.col("ndgprincipale"), dataInizioDefCol, dataFineDefCol,
                dataInizioPdCol, dataInizioIncCol, dataInizioRistruttCol, dataSofferenzaCol, totAccordatoDatDefCol, totUtilizzDatDefCol,
                functions.col("segmento_calc"), functions.col("ciclo_soff"), functions.col("stato_anagrafico"));

        // 127
        String fposiGen2OutCsv = getProperty("fposi.gen2.csv");
        logger.info("fposiGen2OutCsv: " + fposiGen2OutCsv);

        // 129
        fposiGen2.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(
                Paths.get(stepOutputDir, fposiGen2OutCsv).toString());

        // 136

        Column subStringDataInizioDefCol = functions.substring(fposiBase.col("datainiziodef"), 0, 6);
        Column subStringDataFineDefCol = functions.substring(fposiBase.col("datafinedef"), 0, 6);

        // redefine the WindowSpec
        // GROUP fposi_base BY ( ufficio, datarif, codicebanca, segmento_calc, SUBSTRING(datainiziodef,0,6), SUBSTRING(datafinedef,0,6),
        // stato_anagrafico, ciclo_soff, flag_aperto );
        w = Window.partitionBy(fposiBase.col("ufficio"), fposiBase.col("datarif"), fposiBase.col("codicebanca"),
                fposiBase.col("segmento_calc"), subStringDataInizioDefCol, subStringDataFineDefCol, fposiBase.col("stato_anagrafico"),
                fposiBase.col("ciclo_soff"), fposiBase.col("flag_aperto"));

        Dataset<Row> fposiSintGen2 = fposiBase.select(functions.col("ufficio"), functions.col("datarif"),
                functions.col("codicebanca"), functions.col("segmento_calc"), subStringDataInizioDefCol, subStringDataFineDefCol,
                functions.col("stato_anagrafico"), functions.col("ciclo_soff"), functions.col("flag_aperto"),
                functions.count("ufficio").over(w).as("row_count"),
                totAccordatoDatDefCol, totUtilizzDatDefCol);
        // 169

        String fposiSintGen2Csv = getProperty("fposi.sint.gen2");
        logger.info("fposiSintGen2Csv: " + fposiSintGen2Csv);

        fposiSintGen2.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(Paths.get(
                stepOutputDir, fposiSintGen2Csv).toString());
    }

    // column is null?'99999999':column
    private Column getDateColumnCondition(Dataset<Row> df, String colName){

        return functions.when(df.col(colName).isNull(), "99999999").otherwise(df.col(colName));
    }
}
