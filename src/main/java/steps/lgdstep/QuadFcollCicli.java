package steps.lgdstep;

import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import steps.abstractstep.AbstractStep;
import steps.schemas.QuadFcollCicliSchema;

import java.util.*;

import static steps.abstractstep.StepUtils.fromPigSchemaToStructType;
import static steps.abstractstep.StepUtils.selectDfColumns;
import static steps.abstractstep.StepUtils.toScalaColSeq;

public class QuadFcollCicli extends AbstractStep {

    private final Logger logger = Logger.getLogger(QuadFcollCicli.class);

    // required parameters
    private String ufficio;

    public QuadFcollCicli(String ufficio){

        this.ufficio = ufficio;
        stepInputDir = getValue("quad.fcoll.cicli.input.dir");
        stepOutputDir = getValue("quad.fcoll.cicli.output.dir");

        logger.debug("ufficio: " + this.ufficio);
        logger.debug("stepInputDir: " + stepInputDir);
        logger.debug("stepOutputDir: " + stepOutputDir);
    }

    @Override
    public void run() {

        String fcollCsv = getValue("quad.fcoll.cicli.fcoll.csv");
        String cicliNdgLoadCsv = getValue("quad.fcoll.cicli.cicli.ndg.load.csv");
        String fileOutPath = getValue("quad.fcoll.cicli.file.out");

        logger.debug("fcollCsv: " + fcollCsv);
        logger.debug("cicliNdgLoadCsv: " + cicliNdgLoadCsv);
        logger.debug("fileOutCsv: " + fileOutPath);


        Dataset<Row> fcoll = sparkSession.read().format(csvFormat).option("sep", ",")
                .schema(fromPigSchemaToStructType(QuadFcollCicliSchema.getFcollPigSchema()))
                .csv(fcollCsv);

        Dataset<Row> cicliNdgLoad = sparkSession.read().format(csvFormat).option("sep", ",")
                .schema(fromPigSchemaToStructType(QuadFcollCicliSchema.getCicliNdgLoadPigSchema()))
                .csv(cicliNdgLoadCsv);

        // JOIN cicli_ndg_load BY (cd_isti_coll, ndg_coll) FULL OUTER,
        //      fcoll          BY (istituto_collegato, ndg_collegato);

        Column joinCondition = cicliNdgLoad.col("cd_isti_coll").equalTo(fcoll.col("istituto_collegato"))
                .and(cicliNdgLoad.col("ndg_coll").equalTo(fcoll.col("ndg_collegato")));

        List<Column> fileOutSelectList = new ArrayList<>(Collections.singletonList(functions.lit(ufficio).alias("ufficio")));
        fileOutSelectList.addAll(selectDfColumns(cicliNdgLoad, Arrays.asList("ndg_principale", "dt_inizio_ciclo", "cd_isti_coll", "ndg_coll")));

        Map<String, String> fcollSelectMap = new LinkedHashMap<String, String>(){{

            put("codicebanca", "fcoll_codicebanca");
            put("ndgprincipale", "fcoll_ndgprincipale");
            put("istituto_collegato", "fcoll_cd_istituto_coll");
            put("ndg_collegato", "fcoll_ndg_coll");
            put("datainiziodef", "fcoll_data_inizio_def");
        }};

        fileOutSelectList.addAll(selectDfColumns(fcoll, fcollSelectMap));

        Dataset<Row> fileOut = cicliNdgLoad.join(fcoll, joinCondition, "full_outer")
                .select(toScalaColSeq(fileOutSelectList)).distinct();

        fileOut.write().format(csvFormat).option("sep", ",").mode(SaveMode.Overwrite).csv(fileOutPath);
    }
}
