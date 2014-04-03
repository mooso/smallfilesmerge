# Small files merger
This a quick and dirty MR job to merge many small files using a Hadoop Map-Reduce (well - map-only) job.
It should run on any Hadoop cluster, but it has specific optimizations for running against Azure Storage on Azure HDInsight.

# Usage for HDInsight
From a PowerShell window, with mvn and git in the path. Assuming $clust is an HDInsight cluster, and $cont is the default container for it.

    git clone https://github.com/mooso/smallfilesmerge.git
	cd smallfilesmerge
	mvn package
	$inPath = "wasb://myinputcontainer@myaccount.blob.core.windows.net/path/to/files"
	$outPath = "wasb://myoutputcontainer@myaccount.blob.core.windows.net/output/path"
	Set-AzureStorageBlobContent -Blob "jars/microsoft-hadoop-smallfilemerge-0.0.1.jar" -Container $cont.Name -File .\target\microsoft-hadoop-smallfilemerge-0.0.1.jar –Force
	$jobDef = New-AzureHDInsightMapReduceJobDefinition -JarFile "/jars/microsoft-hadoop-smallfilemerge-0.0.1.jar" -ClassName "com.microsoft.hadoop.smallfilesmerge.FileMergerByDirectory" -Arguments $inPath, $outPath
	$job = Start-AzureHDInsightJob -Cluster $clust.Name -JobDefinition $jobDef
	Wait-AzureHDInsightJob -Job $job -WaitTimeoutInSeconds 72000
	Get-AzureHDInsightJobOutput -Cluster $clust.Name -JobId $job.JobId

# Additional features and optimizations
* You can specify multiple input directories/otuput directories as a comma-separated list
* For testing, you can specify three additional arguments: `-popInput <numDirs> <numFiles>`, which will just populate `numDirectories` each with `numFiles` files in them.
* For even faster merge jobs if you have lots of files, you can specify the account key for the input path in the define combine.directory.account.key. This will allow the merger to use storage API directly to more quickly list the input files. Powershell example:

    $defines = @{ "mapred.task.timeout"="6000000"; "combine.directory.account.key"=$(Get-AzureStorageKey $myStorageAccount).Primary }
	$jobDef = New-AzureHDInsightMapReduceJobDefinition -JarFile "/jars/microsoft-hadoop-smallfilemerge-0.0.1.jar" -ClassName "com.microsoft.hadoop.smallfilesmerge.FileMergerByDirectory" -Arguments $inPath, $outPath -Defines $defines
