package io.spring.batch.partitioner;

/**
 * Created by Hosni on 19/03/2017.
 */
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

/**
 * Slightly changed MultiResourcePartitioner, does create output file name too.
 *
 * @author Michael R. Lange <michael.r.lange@langmi.de>
 */
@StepScope
public class CustomMultiResourcePartitioner implements Partitioner {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());
    private static final String PARTITION_KEY = "partition";
    private Resource[] resources = new Resource[0];
    private String inputKeyName = "inputFilePath";
    private String outputKeyName = "outputFileName";
    private String resourceName= "outputFileNam";

    /**
     * Assign the filename of each of the injected resources to an
     * {@link ExecutionContext}.
     *
     * @see Partitioner#partition(int)
     */
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> map = new HashMap<String, ExecutionContext>(gridSize);
        int i = 0;
        for (Resource resource : resources) {
            ExecutionContext context = new ExecutionContext();
            Assert.state(resource.exists(), "Resource does not exist: " + resource);
            //context.putString(inputKeyName, resource.getURL().getPath());

            context.putString(inputKeyName, resource.getFilename());

            String value =(String) context.get(inputKeyName);

            context.putString("resourceName",value);

            context.putString(inputKeyName, resource.getFilename());


            context.put(outputKeyName, createOutputFilename(i, resource));
            map.put(PARTITION_KEY + i, context);
            i++;
        }
        return map;
    }

    /**
     * Creates distinct output file name per partition.
     *
     * @param partitionId
     * @param context
     * @param resource
     * @return
     */
    private String createOutputFilename(int partitionId, Resource resource) {
        String outputFileName = "output-" + String.valueOf(partitionId) + ".xml";
        LOG.info(
                "for inputfile:'"
                        + resource.getFilename()
                        + "' outputfilename:'"
                        + outputFileName
                        + "' was created");

        System.out.println(
                "for inputfile:'"
                        + resource.getFilename()
                        + "' outputfilename:'"
                        + outputFileName
                        + "' was created");

        return outputFileName;
    }

    /**
     * The resources to assign to each partition. In Spring configuration you
     * can use a pattern to select multiple resources.
     *
     * @param resources the resources to use
     */
    public void setResources(Resource[] resources) {
        this.resources = resources;
    }
}