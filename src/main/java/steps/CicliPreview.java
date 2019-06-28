package steps;

import org.apache.commons.cli.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CicliPreview extends AbstractStep{

    // required parameters
    private String dataA;
    private String ufficio;

    CicliPreview(String[] args){

        // define options dataA, ufficio and set them as required
        Option dataAOption = new Option("da", "dataA", true, "parametro dataA");
        Option ufficioOption = new Option("u", "ufficio", true, "parametro ufficio");
        dataAOption.setRequired(true);
        ufficioOption.setRequired(true);

        // add the two previously defined options
        Options options = new Options();
        options.addOption(dataAOption);
        options.addOption(ufficioOption);

        CommandLineParser commandLineParser = new BasicParser();

        // try to parse and retrieve command line arguments
        try{

            CommandLine cmd = commandLineParser.parse(options, args);
            dataA = cmd.getOptionValue("dataA");
            ufficio = cmd.getOptionValue("ufficio");

        } catch (ParseException e) {

            // asign some dafault values
            logger.info("ParseException: " + e.getMessage());
            dataA = "2019-01-01";
            ufficio = "ufficio_bpm";
            logger.info("setting dataA to " + dataA);
            logger.info("setting ufficio to " + ufficio);

        }
    }

    void run(){

        // input path and file name
        String cicliPreviewInputDir = getProperty("CICLI_PREVIEW_INPUT_DIR");
        String fposiOutdirCsv = getProperty("FPOSI_OUTDIR_CSV");
        logger.info("cicliPreviewInputDir: " + cicliPreviewInputDir);
        logger.info("fposiOutdirCsv: " + fposiOutdirCsv);

        // define dataset schema
        List<String> fposiColumns = Arrays.asList(
                "codicebanca", "ndgprincipale", "datainiziodef", "datafinedef", "datainiziopd",
                "datainizioinc", "datainizioristrutt", "datasofferenza", "totaccordatodatdef",
                "totutilizzdatdef", "segmento", "naturagiuridica_segm");
        StructType fposiLoadSchema = setDfSchema(fposiColumns);

        String csvFormat = getProperty("CSV_FORMAT");
        logger.info("csvFormat: " + csvFormat);

        // 21
        String fposiOutdirCsvPath = Paths.get(cicliPreviewInputDir, fposiOutdirCsv).toString();
        logger.info("fposiOutdirCsvPath: " + fposiOutdirCsvPath);
        Dataset<Row> fposiLoad = sparkSession.read().format(csvFormat).option("delimiter", ",").
                schema(fposiLoadSchema).csv(fposiOutdirCsvPath);
        //36

        //53
        // (naturagiuridica_segm != 'CO' AND segmento in ('01','02','03','21')?'IM': (segmento == '10'?'PR':'AL')) as segmento_calc
        Column segmentoCalcCol = functions.when(fposiLoad.col("naturagiuridica_segm").notEqual(functions.lit("CO"))
                .and(fposiLoad.col("segmento").isin("01", "02", "03", "21")), functions.lit("IM")).otherwise(
                        functions.when(fposiLoad.col("segmento").equalTo(functions.lit("10")), functions.lit("PR"))
                                .otherwise(functions.lit("AL"))).as("segmento_calc");

        Column cicloSoffCol = functions.when(fposiLoad.col("datasofferenza").isNull(), functions.lit("N")).otherwise(functions.lit("S"));

        Column dataInizioPdFilterCol = getDateColumnCondition(fposiLoad, "datainiziopd");
        Column dataInizioIncFilterCol = getDateColumnCondition(fposiLoad, "datainizioinc");
        Column dataInizioRistruttFilterCol = getDateColumnCondition(fposiLoad, "datainizioristrutt");
        Column dataSofferenzaFilterCol = getDateColumnCondition(fposiLoad, "datasofferenza");

         /*
        PIG 73
        ( datasofferenza is not null and
        (datasofferenza<(datainiziopd is null?'99999999':datainiziopd) and
         datasofferenza<(datainizioinc is null?'99999999':datainizioinc) and
         datasofferenza<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'SOFF': 'PASTDUE')
        */

        Column dataSofferenzaCaseWhenCol = functions.when(fposiLoad.col("datasofferenza").isNotNull()
                .and(fposiLoad.col("datasofferenza").$less(dataInizioPdFilterCol))
                .and(fposiLoad.col("datasofferenza").$less(dataInizioIncFilterCol))
                .and(fposiLoad.col("datasofferenza").$less(dataInizioRistruttFilterCol)), "SOFF").otherwise("PASTDUE");

        /*
        datainizioristrutt is not null and
        (datainizioristrutt<(datainiziopd is null?'99999999':datainiziopd) and
        datainizioristrutt<(datainizioinc is null?'99999999':datainizioinc) and
        datainizioristrutt<(datasofferenza is null?'99999999':datasofferenza))? 'RISTR'
         */
        Column dataInizioRistruttCaseWhenCol = functions.when(fposiLoad.col("datainizioristrutt").isNotNull()
                .and(fposiLoad.col("datainizioristrutt").$less(dataInizioPdFilterCol))
                .and(fposiLoad.col("datainizioristrutt").$less(dataInizioIncFilterCol))
                .and(fposiLoad.col("datainizioristrutt").$less(dataSofferenzaFilterCol)), "RISTR").otherwise(dataSofferenzaCaseWhenCol);

        /*
        ( datainizioinc is not null and
         (datainizioinc<(datainiziopd is null?'99999999':datainiziopd) and
          datainizioinc<(datasofferenza is null?'99999999':datasofferenza) and
          datainizioinc<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'INCA':
         */
        Column dataInizioIncCaseWhenCol = functions.when(fposiLoad.col("datainizioinc").isNotNull()
                .and(fposiLoad.col("datainizioinc").$less(dataInizioPdFilterCol))
                .and(fposiLoad.col("datainizioinc").$less(dataSofferenzaFilterCol))
                .and(fposiLoad.col("datainizioinc").$less(dataInizioRistruttFilterCol)), "INCA").otherwise(dataInizioRistruttCaseWhenCol);

        /*
        ( datainiziopd is not null and
         (datainiziopd<(datasofferenza is null?'99999999':datasofferenza) and
          datainiziopd<(datainizioinc is null?'99999999':datainizioinc) and
          datainiziopd<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'PASTDUE':

         */
        Column dataInizioPdNotNullCaseWhenCol = functions.when(fposiLoad.col("datainiziopd").isNotNull()
                .and(fposiLoad.col("datainiziopd").$less(dataSofferenzaFilterCol))
                .and(fposiLoad.col("datainiziopd").$less(dataInizioIncFilterCol))
                .and(fposiLoad.col("datainiziopd").$less(dataInizioRistruttFilterCol)), "PASTDUE").otherwise(dataInizioIncCaseWhenCol);

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

        Column flagApertoCol = functions.when(fposiLoad.col("datafinedef").$greater(functions.unix_timestamp(
                functions.lit(dataA), "yyyy-MM-dd")), "A").otherwise("C").as("flag_aperto");

        Dataset<Row> fposiBase = fposiLoad.select(functions.lit(ufficio).as("ufficio"), functions.col("codicebanca"),
                functions.lit(dataA).as("datarif"), functions.col("ndgprincipale"), functions.col("datainiziodef"),
                functions.col("datafinedef"), functions.col("datainiziopd"), functions.col("datainizioinc"),
                functions.col("datainizioristrutt"), functions.col("datasofferenza"), functions.col("totaccordatodatdef"),
                functions.col("totutilizzdatdef"), segmentoCalcCol, cicloSoffCol, statoAnagraficoCol, flagApertoCol);

        // 81

        // 83
        String[] fposiBaseColumnNames = fposiBase.columns();
        List<String> fposiBaseCloneColumnNames = new ArrayList<>();
        for (String fposiBaseColumnName: fposiBaseColumnNames){
            fposiBaseCloneColumnNames.add(fposiBaseColumnName + "_clone");
        }

        // clone fposiBase to avoid Analysis exception
        Dataset<Row> fposiBaseClone = fposiBase.toDF(fposiBaseCloneColumnNames.toArray(new String[0]));
        Dataset<Row> fposiGrp = fposiBaseClone.groupBy("codicebanca_clone", "ndgprincipale_clone", "datainiziodef_clone").agg(
                functions.sum(fposiBase.col("totaccordatodatdef_clone")).as("totaccordatodatdef"),
                functions.sum(fposiBase.col("totutilizzdatdef_clone")).as("totutilizzdatdef"));

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
        Dataset<Row> fposiGen2 = fposiGrp.join(fposiBase, fposiGrp.col("codicebanca_clone").equalTo(fposiBase.col("codicebanca"))
                .and(fposiGrp.col("ndgprincipale_clone").equalTo(fposiBase.col("ndgprincipale")))
                .and(fposiGrp.col("datainiziodef_clone").equalTo(fposiBase.col("datainiziodef"))), "inner")
                .select(fposiBase.col("ufficio"), fposiBase.col("codicebanca"),
                        fposiBase.col("datarif"), fposiBase.col("ndgprincipale"),
                        functions.date_format(fposiBase.col("datainiziodef"), "yyyy-MM-dd").as("datainiziodef"),
                        functions.date_format(fposiBase.col("datafinedef"), "yyyy-MM-dd").as("datafinedef"),
                        functions.date_format(fposiBase.col("datainiziopd"), "yyyy-MM-dd").as("datainiziopd"),
                        functions.date_format(fposiBase.col("datainizioinc"), "yyyy-MM-dd").as("datainizioinc"),
                        functions.date_format(fposiBase.col("datainizioristrutt"), "yyyy-MM-dd").as("datainizioristrutt"),
                        functions.date_format(fposiBase.col("datasofferenza"), "yyyy-MM-dd").as("datasofferenza"),
                        fposiGrp.col("totaccordatodatdef"), fposiGrp.col("totutilizzdatdef"),
                        fposiBase.col("segmento_calc"), fposiBase.col("ciclo_soff"), fposiBase.col("stato_anagrafico"));

        // 127

        String cicliPreviewOutputDir = getProperty("CICLI_PREVIEW_OUTPUT_DIR");
        String fposiGen2OutCsv = getProperty("FPOSI_GEN2_CSV");
        logger.info("cicliPreviewOutputDir: " + cicliPreviewOutputDir);
        logger.info("fposiGen2OutCsv: " + fposiGen2OutCsv);

        // 129
        fposiGen2.write().format(csvFormat).option("delimiter", ",").csv(Paths.get(cicliPreviewOutputDir, fposiGen2OutCsv).toString());

        // 136
        /*
        fposi_sint_grp = GROUP fposi_base BY ( ufficio, datarif, codicebanca, segmento_calc,
            SUBSTRING(datainiziodef,0,6), SUBSTRING(datafinedef,0,6), stato_anagrafico, ciclo_soff, flag_aperto );
         */
        Dataset<Row> fposiSintGen = fposiBase.groupBy(fposiBase.col("ufficio"), fposiBase.col("datarif"),
                fposiBase.col("codicebanca"), fposiBase.col("segmento_calc"), functions.substring(fposiBase.col("datainiziodef"),
                        0, 7), functions.substring(fposiBase.col("datafinedef"), 0, 7), fposiBase.col("stato_anagrafico"),
                fposiBase.col("ciclo_soff"), fposiBase.col("flag_aperto")).max();
    }

    private Column getDateColumnCondition(Dataset<Row> df, String colName){

        return functions.when(df.col(colName).isNull(), "99999999").otherwise(df.col(colName));
    }
}
