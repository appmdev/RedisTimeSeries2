
using StackExchange.Redis;
using NRedisTimeSeries;
using NRedisTimeSeries.Commands.Enums;
using NRedisTimeSeries.DataTypes;
using NetTopologySuite.Geometries;
using NRedisStack.Search.Aggregation;
// See https://aka.ms/new-console-template for more information

var timenow = DateTime.Now;
var timetostring = timenow.ToString();
Console.WriteLine($"Redis Timeseries, Test! start {timetostring}");
var muxer = ConnectionMultiplexer.Connect("localhost");
var db = muxer.GetDatabase();
var totalll = 0;
//var aggregations = new TsAggregation[] { TsAggregation.Avg, TsAggregation.Min, TsAggregation.Max };
//
if (!await db.KeyExistsAsync("location5"))
{
    //2592000000 30 days
    //86400000 1 day
    await db.TimeSeriesCreateAsync("location5", 2592000000, new List<TimeSeriesLabel> { new TimeSeriesLabel("id1", "location-1") });
}

var producerTask = Task.Run(async () => {
    int i = 0;
    int minute = 0;
    while (true)
    {
        i = i + 1;
        await db.TimeSeriesAddAsync("location5", "*", Random.Shared.Next(50));
        await Task.Delay(1000);
        if (i%100 == 0)
        {
            Console.WriteLine($"location5 - seconds:{i}");
        }
        if (i % 300 == 0)
        {
            minute = minute + 5;
            var timenow = DateTime.Now;
            var timetostring = timenow.ToString();
            Console.WriteLine($"Minutes :{minute}; at time {timetostring}");
        }
        //Console.WriteLine($"seconds:{i}");
    }
});
//
async void Location(string location)
{
    if (!await db.KeyExistsAsync(location))
    {
        await db.TimeSeriesCreateAsync(location, 2592000000, new List<TimeSeriesLabel> { new TimeSeriesLabel("id1", "location-1") });
    }

    var producerTask = Task.Run(async () => {
        int i = 0;
        while (true)
        {
            i = i + 1;
            await db.TimeSeriesAddAsync(location, "*", Random.Shared.Next(50));
            await Task.Delay(1000); 
            if (i % 1000 == 0)
            {

                var timenow = DateTime.Now;
                var timetostring = timenow.ToString();
                Console.WriteLine($"{location} - sample:{i}; at time: {timetostring}");
            }
        }
    });
    await Task.WhenAll(producerTask);
}
async void Robot(string robot)
{
    if (!await db.KeyExistsAsync(robot))
    {
        await db.TimeSeriesCreateAsync(robot, 2592000000, new List<TimeSeriesLabel> { new TimeSeriesLabel("id2", "robot-1") });
    }

    var producerTask = Task.Run(async () => {
        int i = 0;
        while (true)
        {
            i = i + 1;
            await db.TimeSeriesAddAsync(robot, "*", Random.Shared.Next(50));
            await Task.Delay(200);
            if (i % 1000 == 0)
            {
                var timenow = DateTime.Now;
                var timetostring = timenow.ToString();
                Console.WriteLine($"{robot} - sample:{i}; at time: {timetostring}");
            }
        }
    });
    await Task.WhenAll(producerTask);
}
async void NetApp(string netapp)
{
    if (!await db.KeyExistsAsync(netapp))
    {
        await db.TimeSeriesCreateAsync(netapp, 2592000000, new List<TimeSeriesLabel> { new TimeSeriesLabel("id3", "netapp-1") });
    }

    var producerTask = Task.Run(async () => {
        int i = 0;
        while (true)
        {
            i = i + 1;
            await db.TimeSeriesAddAsync(netapp, "*", Random.Shared.Next(50));
            await Task.Delay(200);
            if (i % 1000 == 0)
            {
                var timenow = DateTime.Now;
                var timetostring = timenow.ToString();
                Console.WriteLine($"{netapp} - sample:{i}; at time: {timetostring}");
            }
        }
    });
    await Task.WhenAll(producerTask);
}

var tasks = new List<Task>();
/*
for (int i = 0; i < 30; i++)
{
    var producerTask = Task.Run(async (string robot) => {
        int i = 0;
        while (true)
        {
            i = i + 1;
            await db.TimeSeriesAddAsync(robot, "*", Random.Shared.Next(50));
            await Task.Delay(200);
            if (i % 1000 == 0)
            {
                var timenow = DateTime.Now.AddHours(i);
                var timetostring = timenow.ToString();
                Console.WriteLine($"{robot} - sample:{i}; at time: {timetostring}");
            }
        }
    })("robot"+i);
    tasks.Add(producerTask);
}
//*/
//await Task.WhenAll(tasks);

for (int i = 0; i < 17; i++)
{
    for (int j = 0; j < 5; j++)
    {
        var firstJ = j.ToString();
        var secondI = i.ToString();
        var name = "location" + firstJ + "a" + secondI;
        CreateFiles(name);
        await Task.Delay(200);
        //Location(name);
    }
}

for (int i = 0; i < 17; i++)
{
    for (int j = 0; j < 11; j++)
    {
        var firstJ = j.ToString();
        var secondI = i.ToString();

        var name = "robot" + firstJ + "a" + secondI;
        await Task.Delay(200);
        CreateFiles(name);
        //Robot(name);
    }
}
for (int i = 0; i < 17; i++)
{
    for (int j = 0; j < 11; j++)
    {
        var firstJ = j.ToString();
        var secondI = i.ToString();
        var name = "netapp" + firstJ + "a" + secondI;
        await Task.Delay(200);
        CreateFiles(name);
        //NetApp(name);
    }
}

//*/
//await db.TimeSeriesAddAsync(netapp, "*", Random.Shared.Next(50));

//string nameee = "netapp1a7";

//CreateFiles(nameee);
async void CreateFiles(string nameee)
{
    var result1 = await db.TimeSeriesGetAsync(nameee);
    Console.WriteLine($"{nameee}: {result1.Time.Value}: {result1.Val}");
    TimeStamp firstTimestamp = 1703245281391;
    TimeStamp lastTimestamp = 1703252960069;
    var result1Total = await db.TimeSeriesRangeAsync(nameee, firstTimestamp, lastTimestamp, null);
    var result2AverageTot = await db.TimeSeriesRangeAsync(nameee, firstTimestamp, lastTimestamp, null, TsAggregation.Avg, 2592000000);


    var result3Avg2Minutes = await db.TimeSeriesRangeAsync(nameee, firstTimestamp, lastTimestamp, null, TsAggregation.Avg, 120000);
    var result4AvgMin2Minnutes = await db.TimeSeriesRangeAsync(nameee, firstTimestamp, lastTimestamp, null, TsAggregation.Min, 120000);
    var result5AvgMax2Mininutes = await db.TimeSeriesRangeAsync(nameee, firstTimestamp, lastTimestamp, null, TsAggregation.Max, 120000);

    CreateFolder(nameee);
    string path = @"E:\timeseriesResults\" + nameee + @"\" + nameee + "Last.txt";
    string path1 = @"E:\timeseriesResults\" + nameee + @"\" + nameee + "Total.txt";
    string path2 = @"E:\timeseriesResults\" + nameee + @"\" + nameee + "TotalAvg.txt";
    string path3 = @"E:\timeseriesResults\" + nameee + @"\" + nameee + "Avg2Minutes.txt";
    string path4 = @"E:\timeseriesResults\" + nameee + @"\" + nameee + "Minim2Minutes.txt";
    string path5 = @"E:\timeseriesResults\" + nameee + @"\" + nameee + "Max2Minutes.txt";
    string path22 = @"E:\timeseriesResultsAll\" + nameee + ".txt";

    CreateFileAsync(path1, result1Total, nameee, true);
    CreateFileAsync(path22, result1Total,nameee, false);
    CreateFileAsync(path2, result2AverageTot, nameee,false);
    CreateFileAsync(path3, result3Avg2Minutes, nameee, false);
    CreateFileAsync(path4, result4AvgMin2Minnutes, nameee, false);
    CreateFileAsync(path5, result5AvgMax2Mininutes, nameee, false);
}



void CreateFolder(string folderName){
    try
    {
        string path = @"E:\timeseriesResults\" + folderName;
        // Determine whether the directory exists.
        if (Directory.Exists(path))
        {
            //Console.WriteLine("That path exists already.");
            return;
        }
        // Try to create the directory.
        DirectoryInfo di = Directory.CreateDirectory(path);
        //Console.WriteLine("The directory was created successfully at {0}.", Directory.GetCreationTime(path));
    }
    catch (Exception e)
    {
        Console.WriteLine("The process failed: {0}", e.ToString());
    }
    finally { }
}
//string path = @"E:\timeseriesResults\roma\Example.txt";
//string fileName = @"E:\timeseriesResults\roma\Example2.txt";

async void CreateFileAsync(string fileName, IReadOnlyList<TimeSeriesTuple> results, string objectNamee, bool count)
{
    try
    {
        // Check if file already exists. If yes, delete it.
        if (File.Exists(fileName))
        {
            File.Delete(fileName);
        }

        // Create a new file
        using (StreamWriter sw = File.CreateText(fileName))
        {
            var totalResultsNr = results.Count;
            if (count)
            {
                totalll = totalll+ totalResultsNr;
                Console.WriteLine("Total: "+totalll);
            }
            int k = 0;
            await sw.WriteLineAsync("total results for " + objectNamee + "= " + totalResultsNr.ToString());
            foreach (TimeSeriesTuple result in results)
            {
                var time = result.Time.Value.ToString();
                var value = result.Val.ToString();
                var roww = k.ToString() + "- " + time + ": " + value;
                await sw.WriteLineAsync(roww);
                k++;
            }
        }
    }
    catch (Exception Ex)
    {
        Console.WriteLine(Ex.ToString());
    }
}
Console.WriteLine(totalll);
Console.ReadLine();


await Task.WhenAll(producerTask);