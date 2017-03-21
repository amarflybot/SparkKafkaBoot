package com.example;

import com.bazaarvoice.jolt.Chainr;
import com.bazaarvoice.jolt.JsonUtils;
import org.json.JSONException;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.List;

/**
 * Created by amarendra on 21/03/17.
 */
public class TestJsonTranformer {

    @Test
    public void testTransformer() throws JSONException {
        List<Object> specs = JsonUtils.classpathToList("/testSpec.json");
        Chainr chainr = Chainr.fromSpec(specs);

        Object inputJSON = JsonUtils.classpathToObject("/testInput.json");
        Object transformedOutput = chainr.transform(inputJSON);
        String prettyJsonStringResult = JsonUtils.toPrettyJsonString(transformedOutput);
        Object testOut = JsonUtils.classpathToObject("/testOutput.json");
        String testOutString = JsonUtils.toPrettyJsonString(testOut);
        JSONAssert.assertEquals(testOutString,prettyJsonStringResult,false);

    }
}
