use aws_sdk_s3::types::ByteStream;
///! Test for s3 playing
///
use aws_sdk_s3::{Client, Config, Endpoint, Error};
use aws_types::region::Region;
use aws_types::Credentials;
use http::Uri;
use serde_derive::{Deserialize, Serialize};
use std::io::Read;
use std::str::{self, FromStr};
use std::time::SystemTime;

#[derive(Debug)]
pub enum S3Result {
    DeleteFailure(String),
    // DownloadFailure(String),
    FileOpenFail(String),
    HeadError(String),
    Success,
    UploadFailure(String),
}

#[derive(Clone, Deserialize)]
struct S3Configuration {
    backup_s3_access_key_id: String,
    backup_s3_secret_access_key: String,
    backup_s3_bucket: String,
    backup_s3_region: String,
    // Set a custom endpoint, for example if you're using minio or another alternate S3 provider
    backup_s3_endpoint: Option<String>,
    // backup_minio: Option<bool>,
}

impl S3Configuration {
    fn new() -> Self {
        let configpath = std::path::PathBuf::from(String::from("config.toml"));
        let mut confighandle = std::fs::File::open(&configpath).unwrap();
        let mut configcontents = String::new();

        #[allow(clippy::unwrap_used)]
        confighandle.read_to_string(&mut configcontents).unwrap();

        toml::from_str(&configcontents)
            .map_err(|error| eprintln!("Failed to load config file: {:?}", error))
            .unwrap()
    }
}

// snippet-start:[rust.example_code.s3.basics.list_objects]
pub async fn list_objects(client: &Client, bucket_name: &str) -> Result<(), Error> {
    let objects = client.list_objects_v2().bucket(bucket_name).send().await?;
    println!("Objects in bucket:");
    for obj in objects.contents().unwrap_or_default() {
        println!("{:?}", obj.key().unwrap());
    }

    Ok(())
}

fn get_client(creds: Credentials, region: String, endpoint: Option<String>) -> Client {
    let client_config = Config::builder()
        .credentials_provider(creds)
        .region(Region::new(region));
    // set the endpoint if we need to
    let client_config = match endpoint {
        Some(_) => client_config.endpoint_resolver(Endpoint::immutable(
            Uri::from_str(endpoint.unwrap().as_str()).unwrap(),
        )),
        None => client_config,
    };
    Client::from_conf(client_config.build())
}

async fn s3_head_file(
    filename: &str,
    aws_client: Client,
    bucket: &str,
) -> Result<String, S3Result> {
    let head = aws_client
        .head_object()
        .key(filename)
        .bucket(bucket)
        .send()
        .await;

    match head {
        // TODO Reduced struct for nicer data
        Ok(response) => Ok(format!("{:?}", response)),
        Err(error) => Err(S3Result::HeadError(format!(
            "Failed head_object() file: {:?}",
            error
        ))),
    }
}
async fn s3_upload_file(
    filename: &str,
    aws_client: Client,
    bucket: &str,
) -> Result<String, S3Result> {
    let bytestream = match ByteStream::from_path(&filename).await {
        Ok(value) => value,
        Err(error) => {
            return Err(S3Result::FileOpenFail(format!(
                "Failed to open file: {:?}",
                error
            )))
        }
    };

    let upload = aws_client
        .put_object()
        .key(filename)
        .bucket(bucket)
        .body(bytestream)
        .send()
        .await;

    match upload {
        Ok(response) => Ok(format!("{:?}", response)),
        Err(error) => Err(S3Result::UploadFailure(format!(
            "Failed to upload file: {:?}",
            error
        ))),
    }
}

async fn s3_delete_file(
    filename: &str,
    aws_client: Client,
    bucket: &str,
) -> Result<String, S3Result> {
    let delete = aws_client
        .delete_object()
        .key(filename)
        .bucket(bucket)
        .send()
        .await;

    match delete {
        Ok(response) => Ok(format!("{:?}", response)),
        Err(error) => Err(S3Result::DeleteFailure(format!(
            "Failed to upload file: {:?}",
            error
        ))),
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct S3FileInfo {
    etag: String,
    size: u64,
    server_side_encryption: bool,
    version_id: Option<String>,
    last_modified: Option<SystemTime>,
}

// main CLI
#[tokio::main(flavor = "current_thread")]
async fn main() {
    // load the config file
    let configuration = S3Configuration::new();

    // create the creds object
    let creds = Credentials::from_keys(
        configuration.backup_s3_access_key_id,
        configuration.backup_s3_secret_access_key,
        None,
    );

    let aws_client = get_client(
        creds,
        configuration.backup_s3_region,
        configuration.backup_s3_endpoint,
    );

    let bucketlist = aws_client
        .list_objects_v2()
        .bucket(&configuration.backup_s3_bucket)
        .send()
        .await;
    let files = match bucketlist {
        Ok(value) => value,
        Err(error) => {
            eprintln!("Failed to pull files: {:?}", error);
            std::process::exit(1);
        }
    };

    println!("listing files...");
    println!("================");
    for file in files.contents().unwrap_or_default() {
        println!("{}", file.key().unwrap());
    }

    println!("Uploading test_file.txt");
    eprintln!(
        "{:?}",
        s3_upload_file(
            "test_file.txt",
            aws_client.to_owned(),
            configuration.backup_s3_bucket.as_str()
        )
        .await
    );
    println!("HEAD test_file.txt");
    eprintln!(
        "{:?}",
        s3_head_file(
            "test_file.txt",
            aws_client.to_owned(),
            configuration.backup_s3_bucket.as_str()
        )
        .await
    );
    println!("DELETE test_file.txt");
    eprintln!(
        "{:?}",
        s3_delete_file(
            "test_file.txt",
            aws_client.to_owned(),
            configuration.backup_s3_bucket.as_str()
        )
        .await
    );
}
