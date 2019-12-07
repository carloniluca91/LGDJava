package steps.mains;

import steps.lgdstep.Fpasperd;

public class FpasperdMain {

    public static void main(String[] args){

        Fpasperd fpasperd = new Fpasperd(FpasperdMain.class.getSimpleName());
        fpasperd.run();
    }
}
