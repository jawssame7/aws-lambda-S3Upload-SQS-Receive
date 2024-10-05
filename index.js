const {ApiGatewayManagementApiClient, PostToConnectionCommand} = require("@aws-sdk/client-apigatewaymanagementapi")
const {DynamoDBClient} = require("@aws-sdk/client-dynamodb")
const {DynamoDBDocumentClient, ScanCommand} = require("@aws-sdk/lib-dynamodb")
const {S3Client, GetObjectCommand} = require('@aws-sdk/client-s3')
const {getSignedUrl} = require('@aws-sdk/s3-request-presigner');

const apiGatewayManagementApi = new ApiGatewayManagementApiClient({
  apiVersion: '2018-11-29',
  endpoint: process.env.WEBSOCKET_API_ENDPOINT // WebSocket APIのエンドポイントを設定
});

// S3クライアントの初期化
const s3Client = new S3Client({region: process.env.S3_REGION});

exports.handler = async (event) => {
  const fileInfo = [];
  for (const record of event.Records) {
    const s3Records = getS3Record(record);
    if (s3Records) {
      for (const s3Record of s3Records) {
        const size = s3Record.s3.object.size;
        const fileUrl = await createURl(s3Record.s3);
        console.log('fileUrl ▽▽▽▽▽▽▽▽▽ ', fileUrl)
        fileInfo.push({
          fileUrl: fileUrl,
          size: size
        });
      }
    }

    try {
      // WebSocket APIに接続しているクライアントの接続IDを取得 (DynamoDBなどから取得)
      const connectionIds = await getConnectionIds();

      // クライアントにメッセージを送信
      for (const connectionId of connectionIds) {
        const cmd = new PostToConnectionCommand({
          ConnectionId: connectionId,
          Data: JSON.stringify(fileInfo)
        });
        console.log('WEBSOCKET_API_ENDPOINT ■■■■ ', process.env.WEBSOCKET_API_ENDPOINT);
        console.log('Send Message ■■■■■■■■■■■■. ', JSON.stringify(fileInfo))
        await apiGatewayManagementApi.send(cmd);
      }

    } catch (err) {
      console.error(err);
    }
  }
};

// 接続IDを取得する関数 (サンプル)
async function getConnectionIds() {
  // DynamoDBなどから接続IDを取得する処理
  const client = new DynamoDBClient({});
  const docClient = DynamoDBDocumentClient.from(client);
  // process.env.TABLE_NAME
  const command = new ScanCommand({
    TableName: process.env.TABLE_NAME,
    ProjectionExpression: 'connectionId' // 取得する属性を指定
  });
  // スキャン操作を実行
  const data = await docClient.send(command);

  const connectionIds = [];
  for (const item of data.Items) {
    connectionIds.push(item.connectionId);
  }

  console.log('. data▽▽▽▽▽▽▽▽▽▽▽', data);
  return connectionIds;
}

function getS3Record(eventRecord) {
  const bodyJson = JSON.parse(eventRecord?.body || {});
  return bodyJson.Records;
}

async function createURl(s3UploadInfo) {
  // バケット名、オブジェクトキー、有効期限を取得
  const bucketName = s3UploadInfo.bucket.name || process.env.BUCKET_NAME; // 環境変数でバケット名を設定
  const objectKey = s3UploadInfo.object.key; // リクエストのパラメータからオブジェクトキーを取得
  const expiresIn = 172800; // 有効期限は2日（172800秒）

  console.log('bucketName !!!!!!!!!!!', bucketName);
  console.log('objectKey !!!!!!!!!!!', objectKey);

  // 署名付きURLを生成するためのパラメータを定義
  const params = {
    Bucket: bucketName,
    Key: objectKey,
  };

  try {
    // getSignedUrl関数を使って署名付きURLを生成
    const command = new GetObjectCommand(params);
    const signedUrl = await getSignedUrl(s3Client, command, {expiresIn});
    console.log('signedUrl !!!!!!!!!!!', signedUrl);
    return signedUrl;
  } catch (err) {
    console.error(err);
    return null;
  }
}