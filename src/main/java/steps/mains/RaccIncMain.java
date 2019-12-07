package steps.main;

import steps.lgdstep.RaccInc;

public class RaccIncMain {

    public static void main(String[] args){

        RaccInc raccInc = new RaccInc(RaccIncMain.class.getSimpleName());
        raccInc.run();
    }
}
