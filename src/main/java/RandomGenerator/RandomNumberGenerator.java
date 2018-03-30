package RandomGenerator;

import java.util.*;

/**
 * Created by ILIAS on 22/1/2018.
 */

public class RandomNumberGenerator {


    private static final Random rand = new Random();
    private static Set uniqueKeyValuesOfR;
    private static Map<Integer, List<Integer>> mapOfLargeTableKeys = new HashMap<Integer, List<Integer>>();
    private static List<Integer> listOfLargeTableKeys = new ArrayList<Integer>();

    private static int counter = 0;

    /** randBetween is the method we use to generate random keys for the small table R
     *  the range of keys is between 0 and the size that we gave for the table at main class */
    public static int randBetween(int min, int max) {
        return rand.nextInt(max - min + 1) + min;
    }

    /** getRandomKey is the method we use to generate random keys for the large table R
     *  the range of keys is between 0 and the size that we gave for the large table at main class
     *
     *  IMPORTANT. The keys are generated in such a way to ensure 100% 2 joins for each key of small table
     *  That is happening by first generating random keys for the table by checking a map that contains all the possible
     *          unique keys of small Table. After the generation of such a key, we are checking the total number of sum
     *          for this key and if its less than 2 then we add it again, if its more than 2 we are trying to get another
     *          key from this map, if all the keys inside that map are of size of 2, meaning we finished and we ensured
     *          the generation of at least 2 joins for each key.
     *  After the first phase of ensuring 2 joins for each key finish, then we randomly generating keys in range of
     *          0 to the size that we gave for the large table at main class. */
    public static int getRandomKey(int min, int max){

        int randomKey = 0;

        if(counter<=uniqueKeyValuesOfR.size()*2){

            boolean continue_ = true;

            while(continue_){

                randomKey = (Integer) uniqueKeyValuesOfR.toArray() [rand.nextInt (uniqueKeyValuesOfR.size())];
                boolean keyIsContained = mapOfLargeTableKeys.containsKey(randomKey);

                if(keyIsContained){

                    List<Integer> listOfInteger = mapOfLargeTableKeys.get(randomKey);
                    int totalCount = listOfInteger.size();

                    if(totalCount<2){
                        listOfLargeTableKeys = mapOfLargeTableKeys.get(randomKey);
                        listOfLargeTableKeys.add(randomKey);
                        mapOfLargeTableKeys.put(randomKey, listOfLargeTableKeys);
                        listOfLargeTableKeys = new ArrayList<Integer>();
                        continue_ = false;
                    } else {
                        for(List<Integer> list : mapOfLargeTableKeys.values()){
                            if(list.size()<2)
                                continue_ = false;
                        }
                    }

                } else {
                    listOfLargeTableKeys.add(randomKey);
                    mapOfLargeTableKeys.put(randomKey, listOfLargeTableKeys);
                    listOfLargeTableKeys = new ArrayList<Integer>();
                }

            }

            counter+=1;

        } else {

            randomKey = randBetween(min, max);

        }

        return randomKey;
    }


    public static void setUniqueKeyValuesOfR(Set uniqueKeyValuesOfR) {
        RandomNumberGenerator.uniqueKeyValuesOfR = uniqueKeyValuesOfR;
    }


}
