import java.time.LocalDate;

public class prova {

    public static void main(String[] args){

        LocalDate localDate1 = LocalDate.now();
        LocalDate localDate2 = LocalDate.now();
        System.out.println(localDate1.compareTo(localDate2));
    }
}
