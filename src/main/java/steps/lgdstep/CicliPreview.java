package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class CicliPreview extends AbstractStep {

    // required parameters
    private String dataA;
    private String ufficio;
    private String dataSofferenzaUdfName;

    public CicliPreview(String loggerName, String dataA, String ufficio){

        super(loggerName);
        logger = Logger.getLogger(loggerName);

        this.dataSofferenzaUdfName = "dataSofferenzaUdf";
        registerDataSofferenzaUdf(dataSofferenzaUdfName);

        this.dataA = dataA;
        this.ufficio = ufficio;

        stepInputDir = getPropertyValue("cicli.preview.input.dir");
        stepOutputDir = getPropertyValue("cicli.preview.output.dir");

        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
        logger.debug("dataA: " + this.dataA);
        logger.debug("ufficio: " + this.ufficio);
    }

    public void run(){

        String csvFormat = getPropertyValue("csv.format");
        String fposiOutdirCsv = getPropertyValue("fposi.outdir.csv");
        String fposiOutdirCsvPath = Paths.get(stepInputDir, fposiOutdirCsv).toString();

        logger.debug("csvFormat: " + csvFormat);
        logger.debug("fposiOutdirCsv: " + fposiOutdirCsv);
        logger.debug("fposiOutdirCsvPath: " + fposiOutdirCsvPath);

        // 21
        List<String> fposiColumns = Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef", "datafinedef", "datainiziopd", "datainizioinc",
                "datainizioristrutt", "datasofferenza", "totaccordatodatdef", "totutilizzdatdef", "segmento", "naturagiuridica_segm");
        StructType fposiLoadSchema = getStringTypeSchema(fposiColumns);
        Dataset<Row> fposiLoad = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(fposiLoadSchema).csv(fposiOutdirCsvPath);

        //36

        //53
        // (naturagiuridica_segm != 'CO' AND segmento in ('01','02','03','21')?'IM': (segmento == '10'?'PR':'AL')) as segmento_calc
        Column segmentoCalcCol = functions.when(fposiLoad.col("naturagiuridica_segm").notEqual("CO")
                .and(fposiLoad.col("segmento").isin("01", "02", "03", "21")), "IM").otherwise(functions.when(
                        fposiLoad.col("segmento").equalTo("10"), "PR").otherwise("AL")).as("segmento_calc");

        Column cicloSoffCol = functions.when(fposiLoad.col("datasofferenza").isNull(), "N").otherwise("S").as("ciclo_soff");

        // define filtering column conditions ...
        Column dataInizioPdFilterCol = getColumnValueOrDefault(fposiLoad.col("datainiziopd"));
        Column dataInizioIncFilterCol = getColumnValueOrDefault(fposiLoad.col("datainizioinc"));
        Column dataInizioRistruttFilterCol = getColumnValueOrDefault(fposiLoad.col("datainizioristrutt"));
        Column dataSofferenzaFilterCol = getColumnValueOrDefault(fposiLoad.col("datasofferenza"));

         /*
        ( datasofferenza is not null and
        (datasofferenza<(datainiziopd is null?'99999999':datainiziopd) and
         datasofferenza<(datainizioinc is null?'99999999':datainizioinc) and
         datasofferenza<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'SOFF': 'PASTDUE')
        */

        Column dataSofferenzaCaseWhenCol = functions.when(fposiLoad.col("datasofferenza").isNotNull()
                .and(fposiLoad.col("datasofferenza").lt(dataInizioPdFilterCol))
                .and(fposiLoad.col("datasofferenza").lt(dataInizioIncFilterCol))
                .and(fposiLoad.col("datasofferenza").lt(dataInizioRistruttFilterCol)),
                "SOFF").otherwise("PASTDUE");

        /*
        datainizioristrutt is not null and
        (datainizioristrutt<(datainiziopd is null?'99999999':datainiziopd) and
        datainizioristrutt<(datainizioinc is null?'99999999':datainizioinc) and
        datainizioristrutt<(datasofferenza is null?'99999999':datasofferenza))? 'RISTR'
         */

        Column dataInizioRistruttCaseWhenCol = functions.when(fposiLoad.col("datainizioristrutt").isNotNull()
                .and(fposiLoad.col("datainizioristrutt").lt(dataInizioPdFilterCol))
                .and(fposiLoad.col("datainizioristrutt").lt(dataInizioIncFilterCol))
                .and(fposiLoad.col("datainizioristrutt").lt(dataSofferenzaFilterCol)),
                "RISTR").otherwise(dataSofferenzaCaseWhenCol);

        /*
        ( datainizioinc is not null and
         (datainizioinc<(datainiziopd is null?'99999999':datainiziopd) and
          datainizioinc<(datasofferenza is null?'99999999':datasofferenza) and
          datainizioinc<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'INCA':
         */

        Column dataInizioIncCaseWhenCol = functions.when(fposiLoad.col("datainizioinc").isNotNull()
                .and(fposiLoad.col("datainizioinc").lt(dataInizioPdFilterCol))
                .and(fposiLoad.col("datainizioinc").lt(dataSofferenzaFilterCol))
                .and(fposiLoad.col("datainizioinc").lt(dataInizioRistruttFilterCol)),
                "INCA").otherwise(dataInizioRistruttCaseWhenCol);


        /*
        ( datainiziopd is not null and
         (datainiziopd<(datasofferenza is null?'99999999':datasofferenza) and
          datainiziopd<(datainizioinc is null?'99999999':datainizioinc) and
          datainiziopd<(datainizioristrutt is null?'99999999':datainizioristrutt))? 'PASTDUE':

         */
        // Column dataInizioPdTSCol = getUnixTimeStampCol(fposiLoad, "datainiziopd", "yyyyMMdd");
        Column dataInizioPdNotNullCaseWhenCol = functions.when(fposiLoad.col("datainiziopd").isNotNull()
                .and(fposiLoad.col("datainiziopd").lt(dataSofferenzaFilterCol))
                .and(fposiLoad.col("datainiziopd").lt(dataInizioIncFilterCol))
                .and(fposiLoad.col("datainiziopd").lt(dataInizioRistruttFilterCol)),
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

        // ( (int)datafinedef > $data_a ? 'A' : 'C' ) as flag_aperto
        Column flagApertoCol = functions.when(dateGtOtherDate(fposiLoad.col("datafinedef"), "yyyyMMdd",
                dataA, dataAPattern), "A").otherwise("C").as("flag_aperto");

        Dataset<Row> fposiBase = fposiLoad.select(functions.lit(ufficio).as("ufficio"), functions.col("codicebanca"),
                functions.lit(dataA).as("datarif"), functions.col("ndgprincipale"), functions.col("datainiziodef"),
                functions.col("datafinedef"), functions.col("datainiziopd"), functions.col("datainizioinc"),
                functions.col("datainizioristrutt"), functions.col("datasofferenza"), functions.col("totaccordatodatdef"),
                functions.col("totutilizzdatdef"), segmentoCalcCol, cicloSoffCol, statoAnagraficoCol, flagApertoCol);

        // 81

        // 83

        /*
        ,ToString(ToDate(datainiziodef,'yyyyMMdd'),'yyyy-MM-dd') as datainiziodef
        ,ToString(ToDate(datafinedef,'yyyyMMdd'),'yyyy-MM-dd') as datafinedef
        ,ToString(ToDate(datainiziopd,'yyyyMMdd'),'yyyy-MM-dd') as datainiziopd
        ,ToString(ToDate(datainizioinc,'yyyyMMdd'),'yyyy-MM-dd') as datainizioinc
        ,ToString(ToDate(datainizioristrutt,'yyyyMMdd'),'yyyy-MM-dd') as datainizioristrutt
        ,ToString(ToDate(datasofferenza,'yyyyMMdd'),'yyyy-MM-dd') as datasofferenza
         */

        Column dataInizioDefCol = changeDateFormat(fposiBase.col("datainiziodef"), "yyyyMMdd", "yyyy-MM-dd").as("datainiziodef");
        Column dataFineDefCol = changeDateFormat(fposiBase.col("datafinedef"), "yyyyMMdd", "yyyy-MM-dd").as("datafinedef");
        Column dataInizioPdCol = changeDateFormat(fposiBase.col("datainiziopd"), "yyyyMMdd", "yyyy-MM-dd").as("datainiziopd");
        Column dataInizioIncCol = changeDateFormat(fposiBase.col("datainizioinc"), "yyyyMMdd", "yyyy-MM-dd").as("datainizioinc");
        Column dataInizioRistruttCol = changeDateFormat(fposiBase.col("datainizioristrutt"), "yyyyMMdd", "yyyy-MM-dd").as("datainizioristrutt");

        Column dataSofferenzaCol = functions.callUDF(dataSofferenzaUdfName, fposiBase.col("datasofferenza"));

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

        // define WindowSpec in order to compute aggregates on fposiBase without grouping
        WindowSpec w = Window.partitionBy("codicebanca", "ndgprincipale", "datainiziodef");

        // SUM(fposi_base.totaccordatodatdef)      as totaccordatodatdef
        // SUM(fposi_base.totutilizzdatdef)        as totutilizzdatdef
        Column totAccordatoDatDefCol = functions.sum(fposiBase.col("totaccordatodatdef")).over(w).as("totaccordatodatdef");
        Column totUtilizzDatDefCol = functions.sum(fposiBase.col("totutilizzdatdef")).over(w).as("totutilizzdatdef");

        Dataset<Row> fposiGen2 = fposiBase.select(fposiBase.col("ufficio"), fposiBase.col("codicebanca"),
                fposiBase.col("datarif"), fposiBase.col("ndgprincipale"), dataInizioDefCol, dataFineDefCol,
                dataInizioPdCol, dataInizioIncCol, dataInizioRistruttCol, dataSofferenzaCol, totAccordatoDatDefCol, totUtilizzDatDefCol,
                fposiBase.col("segmento_calc"), fposiBase.col("ciclo_soff"), fposiBase.col("stato_anagrafico"));

        // 127

        // 129
        String fposiGen2OutCsv = getPropertyValue("fposi.gen2.csv");
        String fposiGen2OutPath = Paths.get(stepOutputDir, fposiGen2OutCsv).toString();

        logger.debug("fposiGen2OutCsv: " + fposiGen2OutCsv);
        logger.debug("fposiGen2OutPath: " + fposiGen2OutPath);

        fposiGen2.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(fposiGen2OutPath);

        // 136

        Column subStringDataInizioDefCol = functions.substring(fposiBase.col("datainiziodef"), 0, 6);
        Column subStringDataFineDefCol = functions.substring(fposiBase.col("datafinedef"), 0, 6);

        // redefine the WindowSpec
        /*
            GROUP fposi_base BY ( ufficio, datarif, codicebanca, segmento_calc, SUBSTRING(datainiziodef,0,6), SUBSTRING(datafinedef,0,6),
            stato_anagrafico, ciclo_soff, flag_aperto );

             group.ufficio          as ufficio
            ,group.datarif          as datarif
            ,group.flag_aperto      as flag_aperto
            ,group.codicebanca      as codicebanca
            ,group.segmento_calc    as segmento_calc
            ,SUBSTRING(group.$4,0,6) as mese_apertura
            ,SUBSTRING(group.$5,0,6)   as mese_chiusura
            ,group.stato_anagrafico as stato_anagrafico
            ,group.ciclo_soff       as ciclo_soff
            ,COUNT(fposi_base)      as row_count
            ,SUM(fposi_base.totaccordatodatdef) as totaccordatodatdef
            ,SUM(fposi_base.totutilizzdatdef)   as totutilizzdatdef
         */

        w = Window.partitionBy(fposiBase.col("ufficio"), fposiBase.col("datarif"), fposiBase.col("codicebanca"),
                fposiBase.col("segmento_calc"), subStringDataInizioDefCol, subStringDataFineDefCol, fposiBase.col("stato_anagrafico"),
                fposiBase.col("ciclo_soff"), fposiBase.col("flag_aperto"));

        Dataset<Row> fposiSintGen2 = fposiBase.select(fposiBase.col("ufficio"), fposiBase.col("datarif"),
                fposiBase.col("flag_aperto"), fposiBase.col("codicebanca"), fposiBase.col("segmento_calc"),
                subStringDataInizioDefCol, subStringDataFineDefCol,
                fposiBase.col("stato_anagrafico"), fposiBase.col("ciclo_soff"),
                functions.count("ufficio").over(w).as("row_count"),
                totAccordatoDatDefCol, totUtilizzDatDefCol);
        // 169

        String fposiSintGen2Csv = getPropertyValue("fposi.sint.gen2");
        String fposiSintGen2Path = Paths.get(stepOutputDir, fposiSintGen2Csv).toString();

        logger.debug("fposiSintGen2Csv: " + fposiSintGen2Csv);
        logger.debug("fposiSintGen2Path: " + fposiSintGen2Path);

        fposiSintGen2.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(fposiSintGen2Path);
    }

    // column is null?'99999999':column
    private Column getColumnValueOrDefault(Column column){

        return functions.when(column.isNull(), "99999999").otherwise(column);
    }

    private void registerDataSofferenzaUdf(String udfName){

        UDF1<String, String> dataSofferenzaUdf = (UDF1<String, String>) (dataSofferenza) -> {

            if (dataSofferenza != null){
                return dataSofferenza.substring(0, 3).concat("-").concat(dataSofferenza.substring(4, 5))
                        .concat("-").concat(dataSofferenza.substring(6, 7));
        }
            else {
                return null;
            }
        };

        sparkSession.udf().register(udfName, dataSofferenzaUdf, DataTypes.StringType);
    }
}
