package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import steps.abstractstep.AbstractStep;

import java.nio.file.Paths;
import java.util.*;

public class QuadFcollCicli extends AbstractStep {

    // required parameters
    private String ufficio;

    public QuadFcollCicli(String loggerName, String ufficio){

        super(loggerName);
        logger = Logger.getLogger(loggerName);

        this.ufficio = ufficio;

        stepInputDir = getPropertyValue("quad.fcoll.cicli.input.dir");
        stepOutputDir = getPropertyValue("quad.fcoll.cicli.output.dir");

        logger.info("stepInputDir: " + stepInputDir);
        logger.info("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String csvFormat = getPropertyValue("csv.format");
        String fcollCsv = getPropertyValue("fcoll.csv");
        String cicliNdgLoadCsv = getPropertyValue("cicli.ndg.load.csv");
        String fileOutPath = getPropertyValue("file.out");

        logger.info("csvFormat: " + csvFormat);
        logger.info("fcollCsv: " + fcollCsv);
        logger.info("cicliNdgLoadCsv: " + cicliNdgLoadCsv);
        logger.info("fileOutCsv: " + fileOutPath);

        List<String> fcollColumnNames = Arrays.asList("codicebanca", "ndgprincipale", "datainiziodef", "datafinedef",
                "data_default", "istituto_collegato", "ndg_collegato", "data_collegamento", "cumulo");

        Dataset<Row> fcoll = sparkSession.read().format(csvFormat).option("sep", ",").schema(getStringTypeSchema(fcollColumnNames))
                .csv(Paths.get(stepInputDir, fcollCsv).toString());

        List<String> cicliNdgLoadColumnNames = Arrays.asList("cd_isti", "ndg_principale", "dt_inizio_ciclo", "dt_fine_ciclo",
                "datainiziopd", "datainizioristrutt", "datainizioinc", "datainiziosoff", "c_key", "tipo_segmne", "sae_segm",
                "rae_segm", "segmento", "tp_ndg", "provincia_segm", "databilseg", "strbilseg", "attivobilseg", "fatturbilseg",
                "ndg_coll","cd_isti_coll");

        Dataset<Row> cicliNdgLoad = sparkSession.read().format(csvFormat).option("sep", ",").schema(getStringTypeSchema(cicliNdgLoadColumnNames))
                .csv(Paths.get(stepInputDir, cicliNdgLoadCsv).toString());

        // JOIN cicli_ndg_load BY (cd_isti_coll, ndg_coll) FULL OUTER,
        //      fcoll          BY (istituto_collegato, ndg_collegato);

        Column joinCondition = cicliNdgLoad.col("cd_isti_coll").equalTo(fcoll.col("istituto_collegato"))
                .and(cicliNdgLoad.col("ndg_coll").equalTo(fcoll.col("ndg_collegato")));

        List<Column> fileOutSelectList = new ArrayList<>(Collections.singletonList(functions.lit(ufficio).alias("ufficio")));
        fileOutSelectList.addAll(selectDfColumns(cicliNdgLoad, Arrays.asList("ndg_principale", "dt_inizio_ciclo", "cd_isti_coll", "ndg_coll")));

        Map<String, String> fcollSelectMap = new HashMap<>();
        fcollSelectMap.put("codicebanca", "fcoll_codicebanca");
        fcollSelectMap.put("ndgprincipale", "fcoll_ndgprincipale");
        fcollSelectMap.put("istituto_collegato", "fcoll_cd_istituto_coll");
        fcollSelectMap.put("ndg_collegato", "fcoll_ndg_coll");
        fcollSelectMap.put("datainiziodef", "fcoll_data_inizio_def");

        fileOutSelectList.addAll(selectDfColumns(fcoll, fcollSelectMap));

        Dataset<Row> fileOut = cicliNdgLoad.join(fcoll, joinCondition, "full_outer")
                .select(toScalaColSeq(fileOutSelectList)).distinct();

        fileOut.write().format(csvFormat).option("sep", ",").mode(SaveMode.Overwrite).csv(Paths.get(stepOutputDir, fileOutPath).toString());
    }
}
