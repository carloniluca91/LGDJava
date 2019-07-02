package steps;

import org.apache.commons.cli.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class FrappNdgMonthly extends AbstractStep{

    // required parameters
    private String dataA;
    private String periodo;
    private int numeroMesi1;
    private int numeroMesi2;

    FrappNdgMonthly(String[] args){

        // define option dataA, periodo, numeroMesi1, numeroMesi2
        Option dataAOption = new Option("da", "dataA", true, "parametro dataA");
        Option periodoOption = new Option("p", "periodo", true, "parametro periodo");
        Option numeroMesi1Option = new Option("nm1", "numero-mesi-1", true, "parametro numero-mesi-1");
        Option numeroMesi2Option = new Option("nm2", "numero-mesi-2", true, "parametro numero-mesi-2");

        // et them as required
        dataAOption.setRequired(true);
        periodoOption.setRequired(true);
        numeroMesi1Option.setRequired(true);
        numeroMesi2Option.setRequired(true);

        // add them to Options
        Options options = new Options();
        options.addOption(dataAOption);
        options.addOption(periodoOption);
        options.addOption(numeroMesi1Option);
        options.addOption(numeroMesi2Option);


        CommandLineParser commandLineParser = new BasicParser();

        // try to parse and retrieve command line arguments
        try{

            CommandLine cmd = commandLineParser.parse(options, args);
            dataA = cmd.getOptionValue("dataA");
            periodo = cmd.getOptionValue("periodo");
            numeroMesi1 = Integer.parseInt(cmd.getOptionValue("numero-mesi-1"));
            numeroMesi2 = Integer.parseInt(cmd.getOptionValue("numero-mesi-2"));

        }
        catch (ParseException e) {

            logger.info("ParseException: " + e.getMessage());
            dataA = "2018-12-01";
            periodo = "default-periodo";
            numeroMesi1 = 1;
            numeroMesi2 = 2;

            logger.info("Setting dataA to: " + dataA);
            logger.info("Setting periodo to: " + periodo);
            logger.info("Setting numeroMesi1 to:" + numeroMesi1);
            logger.info("Setting numeroMesi2 to: " + numeroMesi2);
        }

    }

    public void run() {

        String csvFormat = getProperty("csv_format");
        String frappNdgMonthlyInputDir = getProperty("FRAPP_NDG_MONTHLY_INPUT_DIR");
        String cicliNdgPathCsv = getProperty("CICLI_NDG_PATH_CSV");
        logger.info("csvFormat: " + csvFormat);
        logger.info("frappNdgMonthlyInputDir: " + frappNdgMonthlyInputDir);
        logger.info("cicliNdgPathCsv: " + cicliNdgPathCsv);

        // 26
        List<String> tlbcidefColumns = Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef", "datafinedef", "datainiziopd",
                "datainizioristrutt", "datainizioinc", "datainiziosoff", "c_key", "tipo_segmne", "sae_segm", "rae_segm", "segmento",
                "tp_ndg", "provincia_segm", "databilseg", "strbilseg", "attivobilseg", "fatturbilseg", "ndg_collegato", "codicebanca_collegato",
                "cd_collegamento", "cd_fiscale", "dt_rif_udct");

        StructType tlbcidefSchema = setDfSchema(tlbcidefColumns);
        String cicliNdgPathCsvPath = Paths.get(frappNdgMonthlyInputDir, cicliNdgPathCsv).toString();
        logger.info("cicliNdgPathCsvPath: " + cicliNdgPathCsvPath);

        Dataset<Row> tlbcidef = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(tlbcidefSchema).csv(cicliNdgPathCsvPath);
        // 53

        // 58
        Dataset<Row> cicliNdgPrinc = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNull());
        Dataset<Row> cicliNdgColl = tlbcidef.filter(tlbcidef.col("cd_collegamento").isNotNull());
        // 60

        // 69
        List<String> tlburttColumns = Arrays.asList("cd_istituto", "ndg", "sportello", "conto", "progr_segmento", "dt_riferimento", "conto_esteso",
                "forma_tecnica", "flag_durata_contr", "cred_agevolato", "operazione_pool", "dt_accensione", "dt_estinzione", "dt_scadenza",
                "organo_deliber", "dt_delibera", "dt_scad_fido", "origine_rapporto", "tp_ammortamento", "tp_rapporto", "period_liquid",
                "freq_remargining", "cd_prodotto_ris", "durata_originaria", "divisa", "score_erogaz", "durata_residua", "categoria_sof",
                "categoria_inc", "dur_res_default", "flag_margine", "dt_entrata_def", "tp_contr_rapp", "cd_eplus", "r792_tipocartol");

        StructType tlburttSchema = setDfSchema(tlburttColumns);
        String tlburttCsv = getProperty("TLBURTT_CSV");
        String tlburttCsvPath = Paths.get(frappNdgMonthlyInputDir, tlburttCsv).toString();
        logger.info("tlburttCsv: " + tlburttCsv);
        logger.info("tlburttCsvPath: " + tlburttCsvPath);

        Dataset<Row> tlburtt = sparkSession.read().format(csvFormat).option("delimiter", ",").schema(tlburttSchema).csv(tlburttCsvPath);
        // 107

        // 111
        Dataset<Row> tlburttFilter = tlburtt.filter(tlburtt.col("progr_segmento").equalTo(0));

        // ToDate((chararray)dt_riferimento,'yyyyMMdd') >= SubtractDuration(ToDate((chararray)datainiziodef,'yyyyMMdd'),'$numero_mesi_1')
        Column dtRiferimentoFilterCol = getUnixTimeStampCol(tlburttFilter.col("dt_riferimento"), "yyyyMMdd").$greater$eq(
                getUnixTimeStampCol(functions.add_months(cicliNdgPrinc.col("datainiziodef"), -numeroMesi1), "yyyyMMdd"));

        Column dataFineDefCol = functions.substring(functions.add_months(leastDate(functions.add_months(cicliNdgPrinc.col("datafinedef"), -1),
                functions.date_format(functions.lit(dataA), "yyyyMMdd"), "yyyyMMdd"), numeroMesi2), 0, 6);

        Column dataFineDefFilterCol = getUnixTimeStampCol(tlburttFilter.col("dt_riferimento"), "yyyyMMdd").$less$eq(
                getUnixTimeStampCol(dataFineDefCol, "yyyyMMdd"));

        Dataset<Row> tlbcidefUrttPrinc = cicliNdgPrinc.join(tlburttFilter, cicliNdgPrinc.col("codicebanca_collegato").equalTo(
                tlburttFilter.col("cd_istituto")).and(cicliNdgPrinc.col("ndg_collegato").equalTo(tlburttFilter.col("ndg"))))
                .filter(dtRiferimentoFilterCol.and(dataFineDefFilterCol))
                .select();
                //TODO


    }
}
