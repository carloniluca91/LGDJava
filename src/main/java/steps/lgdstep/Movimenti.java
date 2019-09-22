package steps.lgdstep;

import org.apache.commons.cli.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import scala.collection.Seq;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class Movimenti extends AbstractStep {

    // required parameter
    private String dataOsservazione;

    public Movimenti(String[] args){

        logger = Logger.getLogger(this.getClass().getName());

        Option dataOsservazioneOption = new Option("dO", "dataOsservazione", true, "parametro $data_osservazione");
        dataOsservazioneOption.setRequired(true);

        Options movimentiOptions = new Options();
        movimentiOptions.addOption(dataOsservazioneOption);

        CommandLineParser commandLineParser = new BasicParser();

        try {

            CommandLine commandLine = commandLineParser.parse(movimentiOptions, args);
            dataOsservazione = commandLine.getOptionValue("dataOsservazione");

        } catch (ParseException e) {

            logger.info("ParseException: " + e.getMessage());
            dataOsservazione = "2018-01-01";
        }

        stepInputDir = getProperty("MOVIMENTI_INPUT_DIR");
        stepOutputDir = getProperty("MOVIMENTI_OUTPUT_DIR");

        logger.info("stepInputDir: " + stepInputDir);
        logger.info("stepOutputDir: " + stepOutputDir);
        logger.info("dataOsservazione: " + dataOsservazione);
    }

    @Override
    public void run() {

        String csvFormat = getProperty("csv_format");
        String tlbmovcontaCsv = getProperty("TLBMOVCONTA_CSV");

        logger.info("csvFormat: " + csvFormat);
        logger.info("tlbmovcontaCsv: " + tlbmovcontaCsv);

        List<String> tlbmovcontaColumnNames = Arrays.asList("mo_dt_riferimento", "mo_istituto", "mo_ndg", "mo_sportello",
                "mo_conto", "mo_conto_esteso", "mo_num_soff", "mo_cat_rapp_soff", "mo_fil_rapp_soff", "mo_num_rapp_soff",
                "mo_id_movimento", "mo_categoria", "mo_causale", "mo_dt_contabile", "mo_dt_valuta", "mo_imp_movimento",
                "mo_flag_extracont", "mo_flag_storno", "mo_ndg_principale", "mo_dt_inizio_ciclo");

        StructType tlbmovContaSchema = getDfSchema(tlbmovcontaColumnNames);
        Dataset<Row> tlbmovconta = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(tlbmovContaSchema)
                .csv(Paths.get(stepInputDir, tlbmovcontaCsv).toString());

        // FILTER tlbmovconta BY mo_dt_contabile <= $data_osservazione;
        Column filterCondition = getUnixTimeStampCol(tlbmovconta.col("mo_dt_contabile"), "yyyyMMdd")
                .leq(getUnixTimeStampCol(functions.lit(dataOsservazione), "yyyy-MM-dd"));

        Map<String, String> selectColMap = new HashMap<>();
        selectColMap.put("mo_istituto", "istituto");
        selectColMap.put("mo_ndg", "ndg");
        selectColMap.put("mo_dt_riferimento", "datariferimento");
        selectColMap.put("mo_sportello", "sportello");
        selectColMap.put("mo_conto_esteso", "conto");
        selectColMap.put("mo_num_soff", "numerosofferenza");
        selectColMap.put("mo_cat_rapp_soff", "catrappsoffer");
        selectColMap.put("mo_fil_rapp_soff", "filrappsoffer");
        selectColMap.put("mo_num_rapp_soff", "numrappsoffer");
        selectColMap.put("mo_id_movimento", "idmovimento");
        selectColMap.put("mo_categoria", "categoria");
        selectColMap.put("mo_causale", "causale");
        selectColMap.put("mo_dt_contabile", "dtcontab");
        selectColMap.put("mo_dt_valuta", "dtvaluta");
        selectColMap.put("mo_imp_movimento", "importo");
        selectColMap.put("mo_flag_extracont", "flagextracontab");
        selectColMap.put("mo_flag_storno", "flagstorno");

        Seq<Column> selectColSeq = toScalaColSeq(selectDfColumns(tlbmovconta, selectColMap));
        Dataset<Row> movOutDist = tlbmovconta.filter(filterCondition).select(selectColSeq).distinct();

        String movOutDistPath = getProperty("MOV_OUT_DIST");
        logger.info("movOutDistPath: " + movOutDistPath);

        movOutDist.write().format(csvFormat).option("delimiter", ",").mode(SaveMode.Overwrite).csv(
                Paths.get(stepOutputDir, movOutDistPath).toString());

    }
}
