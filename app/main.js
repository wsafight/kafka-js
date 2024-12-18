import net from "net";

import { sendResponseMessage, pick } from "./utils.js";
import { handleApiVersionsRequest } from "./api_version_request.js";
import { parseRequest } from "./request_parser.js";
import { handleDescribeTopicPartitionsRequest } from "./describe_topic_partitions_request.js";

const server = net.createServer((connection) => {
  connection.on("data", (buffer) => {
    const { messageSize, requestApiKey, requestApiVersion, correlationId } =
      parseRequest(buffer);

    const responseMessage = {
      messageSize,
      requestApiKey,
      requestApiVersion,
      correlationId,
    };

    const requestVersion = requestApiVersion.readInt16BE();

    if (requestApiKey.readInt16BE() === 18) {
      handleApiVersionsRequest(connection, responseMessage, requestVersion);
    } else if (requestApiKey.readInt16BE() === 75) {
      handleDescribeTopicPartitionsRequest(connection, responseMessage, buffer);
    } else {
      sendResponseMessage(
        connection,
        pick(responseMessage, "messageSize", "correlationId"),
      );
    }
  });
});

server.listen(9092, "127.0.0.1");