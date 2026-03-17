def test_copy_file(spark, workspace_client):
    from ghcn.copy_files import copy_files

    source = "s3://noaa-ghcn-pds/readme.txt"
    destination = "/Volumes/ghcn_dev/bronze/raw/readme.txt"

    copy_files(workspace_client, source, destination)

    result = spark.sql("LIST '/Volumes/ghcn_dev/bronze/raw/'").collect()
    file_names = [row.path for row in result]
    assert any("readme.txt" in path for path in file_names)
