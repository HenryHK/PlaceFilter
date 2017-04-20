package analysisTask2.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import analysisTask2.PlaceFilterMapper;

//A simple unit test for PlaceFilterMapper
//It contains a single test method for valid record that emit one key value pair as output
//@author Zhao Liu

public class PlaceFilterMapperTest 
{
	@Test
	public void processValidRecord() throws IOException
	{
		PlaceFilterMapper pfMapper = new PlaceFilterMapper();
		Text input = new Text("xPOpvDeYBJkdlw	15596	51.504	-0.609	Slough, England, GB, United Kingdom	8	/United+Kingdom/England/Slough/Chalvey");
		Mapper.Context context = mock(Mapper.Context.class);
		try
		{
			pfMapper.map(null, input, context);		
			verify(context).write(new Text("xPOpvDeYBJkdlw"), new Text("Slough, England, GB, United Kingdom"));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

}
