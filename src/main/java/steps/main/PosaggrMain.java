package steps.main;

import steps.lgdstep.Posaggr;

public class PosaggrMain {

    public static void main(String[] args){

        Posaggr posaggr = new Posaggr(PosaggrMain.class.getSimpleName());
        posaggr.run();
    }
}
