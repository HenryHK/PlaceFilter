package analysisTask2.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import java.util.Hashtable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import analysisTask2.ReplicateJoinMapper;

//A simple unit test for ReplicateJoinMapper
//It contains a single test method for valid record that emit one key value pair
//@author Zhao Liu

public class ReplicateJoinMapperTest 
{
	@Test
	public void processValidRecord() throws IOException
	{
		Hashtable <String, String> testTable = new Hashtable<String, String>();
		testTable.put("N2Z02BGcB5zqf0_Lpw", "Batur Utara, Bali, Indonesia");
		ReplicateJoinMapper rjMapper = new ReplicateJoinMapper();
		rjMapper.setPlaceTable(testTable);
		Text input = new Text("4641583671	12052211@N04	november bw white black leaves rain foglie may bn pioggia bianco nero	2010-05-23 15:21:48	N2Z02BGcB5zqf0_Lpw	0");
		Mapper.Context context = mock(Mapper.Context.class);
		try
		{
			rjMapper.map(null, input, context);
			verify(context).write(new Text("Indonesia"), new Text("Batur Utara" + "\t" + "12052211@N04"));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

}
