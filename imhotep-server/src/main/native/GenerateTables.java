
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;

/**
 * @author jplaisance
 */
public final class GenerateTables {

    private static final Logger log = Logger.getLogger(GenerateTables.class);

    public static void main(String[] args) {
        int[] vec_lookup = new int[4096];
        List<int[]> vectors = Lists.newArrayList();
        int[] bytesConsumed = new int[180];
        make6table(vec_lookup, vectors, bytesConsumed);
        make4table(vec_lookup, vectors, bytesConsumed);
        make2table(vec_lookup, vectors, bytesConsumed);
        for (int i = 0; i < 4096/32; i++) {
            final StringBuilder stb = new StringBuilder();
            for (int j = 0; j < 32; j++) {
                stb.append(vec_lookup[i*32+j]).append(", ");
            }
            System.out.println(stb);
        }
        final StringBuilder vectorsString = new StringBuilder();
        for (int[] vector : vectors) {
            final StringBuilder vectorString = new StringBuilder("vectors["+vector[16]+"] = _mm_setr_epi8(");
            final String str = Arrays.toString(Arrays.copyOfRange(vector, 0, 16));
            vectorString.append(str.substring(1, str.length()-1));
            vectorString.append(");");
            vectorsString.append(vectorString).append("\n");
        }
        System.out.println(vectorsString);
        for (int i = 0; i < bytesConsumed.length; i+=16) {
            final StringBuilder stb = new StringBuilder();
            for (int j = 0; j < Math.min(16, bytesConsumed.length-i); j++) {
                stb.append(bytesConsumed[i+j]).append(", ");
            }
            System.out.println(stb);
        }
    }

    public static void make6table(int[] vec_lookup, List<int[]> vectors, int[] bytesConsumed) {
        int count = 109;
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                for (int k = 0; k < 2; k++) {
                    for (int l = 0; l < 2; l++) {
                        for (int m = 0; m < 2; m++) {
                            for (int n = 0; n < 2; n++) {
                                int bits = 0;
                                final int[] vector = new int[17];
                                Arrays.fill(vector, -1);
                                vector[16] = count;
                                int current = 0;
                                vector[0] = current++;
                                if (i == 1) {
                                    bits = (bits << 1) | 1;
                                    vector[1] = current++;
                                }
                                bits = bits << 1;
                                vector[2] = current++;
                                if (j == 1) {
                                    bits = (bits << 1) | 1;
                                    vector[3] = current++;
                                }
                                bits = bits << 1;
                                vector[4] = current++;
                                if (k == 1) {
                                    bits = (bits << 1) | 1;
                                    vector[5] = current++;
                                }
                                bits = bits << 1;
                                vector[6] = current++;
                                if (l == 1) {
                                    bits = (bits << 1) | 1;
                                    vector[7] = current++;
                                }
                                bits = bits << 1;
                                vector[8] = current++;
                                if (m == 1) {
                                    bits = (bits << 1) | 1;
                                    vector[9] = current++;
                                }
                                bits = bits << 1;
                                vector[10] = current++;
                                if (n == 1) {
                                    bits = (bits << 1) | 1;
                                    vector[11] = current++;
                                }
                                bits = bits << 1;
                                vectors.add(vector);
                                final int replace = 6 - i - j - k - l - m - n;
                                for (int r = 0; r < (1<<replace); r++) {
                                    if (i == 0 && j == 0 && k == 0 && l == 0 && m == 0 && n == 0) {
                                        final int index = (bits << replace) + r;
                                        System.out.println(String.format("%12s", Integer.toBinaryString(index)).replaceAll(" ", "0"));
                                        final int reverse = Integer.reverse(index) >>> 20;
                                        final int nBytes = Integer.numberOfTrailingZeros(reverse | 0x1000);
                                        vec_lookup[reverse] = nBytes+167;
                                        bytesConsumed[nBytes+167] = nBytes;
                                    } else {
                                        final int index = (bits << replace) + r;
                                        System.out.println(String.format("%12s", Integer.toBinaryString(index)).replaceAll(" ", "0"));
                                        vec_lookup[Integer.reverse(index)>>>20] = count;
                                    }
                                }
                                bytesConsumed[count] = i+j+k+l+m+n+6;
                                count++;
                            }
                        }
                    }
                }
            }
        }
    }

    public static void make4table(int[] vec_lookup, List<int[]> vectors, int[] bytesConsumed) {
        int count = 27;
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                for (int k = 0; k < 3; k++) {
                    for (int l = 0; l < 3; l++) {
                        int bits = 0;
                        final int[] vector = new int[17];
                        Arrays.fill(vector, -1);
                        vector[16] = count;
                        int current = 0;
                        vector[0] = current++;
                        for (int ii = 0; ii < i; ii++) {
                            bits = (bits << 1) | 1;
                            vector[ii+1] = current++;
                        }
                        bits = bits << 1;
                        vector[4] = current++;
                        for (int jj = 0; jj < j; jj++) {
                            bits = (bits << 1) | 1;
                            vector[jj+5] = current++;
                        }
                        bits = bits << 1;
                        vector[8] = current++;
                        for (int kk = 0; kk < k; kk++) {
                            bits = (bits << 1) | 1;
                            vector[kk+9] = current++;
                        }
                        bits = bits << 1;
                        vector[12] = current++;
                        for (int ll = 0; ll < l; ll++) {
                            bits = (bits << 1) | 1;
                            vector[ll+13] = current++;
                        }
                        bits = bits << 1;
                        vectors.add(vector);
                        final int replace = 8 - i - j - k - l;
                        for (int r = 0; r < (1<<replace); r++) {
                            if (i == 0 && j == 0 && k == 0 && l == 0) {
                                final int index = (bits << replace) + r;
                                System.out.println(String.format("%12s", Integer.toBinaryString(index)).replaceAll(" ", "0"));
                                final int reverse = Integer.reverse(index) >>> 20;
                                final int nBytes = 4;
                                if (vec_lookup[reverse] == 0) {
                                    vec_lookup[reverse] = 108;
                                }
                                bytesConsumed[108] = nBytes;
                            } else {
                                final int index = (bits << replace) + r;
                                System.out.println(String.format("%12s", Integer.toBinaryString(index)).replaceAll(" ", "0"));
                                final int reverse = Integer.reverse(index) >>> 20;
                                if (vec_lookup[reverse] == 0) {
                                    vec_lookup[reverse] = count;
                                }
                            }
                        }
                        bytesConsumed[count] = i+j+k+l+4;
                        count++;
                    }
                }
            }
        }
    }

    public static void make2table(int[] vec_lookup, List<int[]> vectors, int[] bytesConsumed) {
        int count = 0;

        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                int bits = 0;
                final int[] vector = new int[17];
                Arrays.fill(vector, -1);
                vector[16] = count;
                int current = 0;
                vector[7] = current++;
                for (int ii = 0; ii < i; ii++) {
                    bits = (bits << 1) | 1;
                    vector[ii*2] = current++;
                }
                bits = bits << 1;
                vector[15] = current++;
                for (int jj = 0; jj < j; jj++) {
                    bits = (bits << 1) | 1;
                    vector[8+jj*2] = current++;
                }
                bits = bits << 1;
                vectors.add(vector);
                final int replace = 10 - i - j;
                for (int r = 0; r < (1<<replace); r++) {
                    if (i == 0 && j == 0) {
                        final int index = (bits << replace) + r;
                        System.out.println(String.format("%12s", Integer.toBinaryString(index)).replaceAll(" ", "0"));
                        final int reverse = Integer.reverse(index) >>> 20;
                        final int nBytes = (reverse & 4) > 0 ? 2 : 3;
                        if (vec_lookup[reverse] == 0) {
                            vec_lookup[reverse] = 23+nBytes;
                        }
                        bytesConsumed[23+nBytes] = nBytes;
                    } else {
                        final int index = (bits << replace) + r;
                        System.out.println(String.format("%12s", Integer.toBinaryString(index)).replaceAll(" ", "0"));
                        final int reverse = Integer.reverse(index) >>> 20;
                        if (vec_lookup[reverse] == 0) {
                            vec_lookup[reverse] = count;
                        }
                    }
                }
                bytesConsumed[count] = i+j+2;
                count++;
            }
        }
    }
}
