/**
 * Created by WaylinWang on 3/11/16.
 */
public class test {
    public static void main(String args[]) {
        int[] outputIterations = {1, 2, 3, 1, 2, 4, 1, 2, 5, 1, 3, 4, 1, 3, 5, 1, 4, 5,
                2, 3, 4, 2, 3, 5, 2, 4, 5, 3, 4, 5};
        for(int i = 0; i < outputIterations.length - 2; i+=3) {
            System.out.println(outputIterations[i] + " " + outputIterations[i+1] + " " + outputIterations[i+2]);
        }

        String t = "        12";
        Long temp = Long.parseLong(t.trim());
        System.out.println(temp);

        for (int i = 1; i < 11 - 1; i+=2) {
            System.out.println(i);
        }

        String input = "2 15.26   14.84   0.871   5.763   3.312   2.221   5.22    1";

        String []out = input.split("\\s+");

        String outVal = "";
        for (int i = 1; i < out.length - 1; i++) {
            outVal+=out[i]+",";
        }
        outVal = outVal.substring(0, outVal.length()-1);
        System.out.println(outVal);

    }
}
