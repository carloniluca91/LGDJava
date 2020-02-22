package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import scala.collection.Seq;
import steps.abstractstep.AbstractStep;
import steps.schemas.MovimentiSchema;

import java.util.LinkedHashMap;
import java.util.Map;

import static steps.abstractstep.StepUtils.changeDateFormat;
import static steps.abstractstep.StepUtils.fromPigSchemaToStructType;
import static steps.abstractstep.StepUtils.selectDfColumns;
import static steps.abstractstep.StepUtils.toScalaColSeq;

public class Movimenti extends AbstractStep {

    private final Logger logger = Logger.getLogger(Movimenti.class);

    // required parameter
    private String dataOsservazione;

    public Movimenti(String dataOsservazione){

        this.dataOsservazione = dataOsservazione;
        stepInputDir = getValue("movimenti.input.dir");
        stepOutputDir = getValue("movimenti.output.dir");

        logger.debug("dataOsservazione: " + this.dataOsservazione);
        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String dataOsservazionePattern = getValue("params.dataosservazione.pattern");
        String tlbmovcontaCsv = getValue("movimenti.tlbmovconta.csv");
        String movOutDistPath = getValue("movimenti.mov.out.dist");

        logger.debug("dataOsservazionePattern: " + dataOsservazionePattern);
        logger.debug("tlbmovcontaCsv: " + tlbmovcontaCsv);
        logger.debug("movOutDistPath: " + movOutDistPath);

        Dataset<Row> tlbmovconta = readCsvAtPathUsingSchema(tlbmovcontaCsv,
                fromPigSchemaToStructType(MovimentiSchema.getTlbmovcontaPigSchema()));

        // FILTER tlbmovconta BY mo_dt_contabile <= $data_osservazione;
        Column dataOsservazioneCol = functions.lit(changeDateFormat(dataOsservazione, dataOsservazionePattern, "yyyyMMdd"));
        Column filterCondition = tlbmovconta.col("mo_dt_contabile").leq(dataOsservazioneCol);

        Map<String, String> selectColMap = new LinkedHashMap<String, String>(){{

            put("mo_istituto", "istituto");
            put("mo_ndg", "ndg");
            put("mo_dt_riferimento", "datariferimento");
            put("mo_sportello", "sportello");
            put("mo_conto_esteso", "conto");
            put("mo_num_soff", "numerosofferenza");
            put("mo_cat_rapp_soff", "catrappsoffer");
            put("mo_fil_rapp_soff", "filrappsoffer");
            put("mo_num_rapp_soff", "numrappsoffer");
            put("mo_id_movimento", "idmovimento");
            put("mo_categoria", "categoria");
            put("mo_causale", "causale");
            put("mo_dt_contabile", "dtcontab");
            put("mo_dt_valuta", "dtvaluta");
            put("mo_imp_movimento", "importo");
            put("mo_flag_extracont", "flagextracontab");
            put("mo_flag_storno", "flagstorno");
        }};

        Seq<Column> selectColSeq = toScalaColSeq(selectDfColumns(tlbmovconta, selectColMap));
        Dataset<Row> movOutDist = tlbmovconta.filter(filterCondition).select(selectColSeq).distinct();
        writeDatasetAsCsvAtPath(movOutDist, movOutDistPath);

    }
}
