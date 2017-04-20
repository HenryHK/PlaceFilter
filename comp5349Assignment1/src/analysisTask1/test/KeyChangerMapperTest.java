package analysisTask1.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;

import analysisTask1.KeyChangerMapper;

//A unit test for KeyChangerMapper
//It contains a single test method for valid record that emit one key value pair
//@author Zhao Liu

public class KeyChangerMapperTest 
{
	@Test
	public void processValidRecord() throws IOException
	{
		
		KeyChangerMapper kcMapper = new KeyChangerMapper();
		Text input = new Text("Abang, Bali, Indonesia	14		(paris:8)	(2009:5)	(75013:4)	(5dmk2:3)	(canon:3)	(leica:3)	(m8:3)	(1977:2)	(aslongasheliesperfectlystill:2)	(bali:2)");
		Mapper.Context context = mock(Mapper.Context.class);
		try
		{
			kcMapper.map(null, input, context);
			verify(context).write(new Text("14"), new Text("Abang, Bali, Indonesia" + "\t" + "(paris:8)	(2009:5)	(75013:4)	(5dmk2:3)	(canon:3)	(leica:3)	(m8:3)	(1977:2)	(aslongasheliesperfectlystill:2)	(bali:2)"));
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

	}

}
