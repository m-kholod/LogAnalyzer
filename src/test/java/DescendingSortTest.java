import org.apache.hadoop.io.IntWritable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DescendingSortTest {
    IntWritable k1;
    IntWritable k2;
    DescendingSortComparator comparator;
    @Before
    public void setUp() throws Exception {
        k1 = new IntWritable(2);
        k2 = new IntWritable(20);
        comparator = new DescendingSortComparator();
    }
    @Test
    public void testAdd() {
        Assert.assertEquals(1, comparator.compare(k1, k2));
    }
}