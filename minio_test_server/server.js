const express = require("express");
const cors = require("cors");
const bodyParser = require("body-parser");
const Minio = require("minio");
const fs = require("fs");
const path = require("path");

const app = express();

const minioClient = new Minio.Client({
  endPoint: "localhost", // Change to your MinIO server endpoint
  port: 9000, // Change to your MinIO server port
  useSSL: false,
  accessKey: "minioadmin", // Change to your access key
  secretKey: "minioadmin", // Change to your secret key
});

let ongoingUploads = new Map();
const bucketName = "uploads";

createBucketIfNotExists(bucketName);

async function createBucketIfNotExists(bucketName) {
  try {
    const exists = await minioClient.bucketExists(bucketName);
    if (!exists) {
      await minioClient.makeBucket(bucketName);
    }
  } catch (error) {
    throw error;
  }
}

app.use(cors());
app.use(bodyParser.raw({ limit: "10mb", type: "application/octet-stream" }));

// Endpoint to delete partial uploads
app.post("/api/upload/cancel", async (req, res) => {
  const fileName = req.headers["x-file-name"]
    ? Buffer.from(req.headers["x-file-name"], "base64").toString()
    : null;
  if (!fileName) {
    return res.status(400).send("File name required");
  }

  const uploadStatus = ongoingUploads.get(fileName) || {
    uploadedChunks: new Set(),
    pendingChunks: new Set(),
    canceled: false,
  };
  uploadStatus.canceled = true;
  ongoingUploads.set(fileName, uploadStatus);

  // Wait for ongoing uploads to complete
  const checkInterval = setInterval(async () => {
    if (uploadStatus.pendingChunks.size === 0) {
      clearInterval(checkInterval);
      try {
        const objectsStream = minioClient.listObjects(
          bucketName,
          fileName,
          true
        );
        for await (const obj of objectsStream) {
          if (obj.name.startsWith(fileName)) {
            await minioClient.removeObject(bucketName, obj.name);
          }
        }
        console.log("Canceled and deleted partial uploaded chunks");
        res
          .status(200)
          .send("Canceled upload and deleted partial uploaded chunks");
      } catch (error) {
        console.error("Error canceling upload:", error);
        res.status(500).send("Error canceling upload");
      }
    }
  }, 1000); // Check every second
});

// Endpoint to delete all files
let isDeletingAll = false;

app.delete("/api/upload/deleteAll", async (req, res) => {
  if (isDeletingAll) {
    return res.status(400).send("Deletion already in progress");
  }

  try {
    isDeletingAll = true;

    const bucketExists = await minioClient.bucketExists(bucketName);
    if (!bucketExists) {
      return res.status(404).send("Bucket not found");
    }

    const objectsStream = minioClient.listObjects(bucketName, "", true);
    for await (const obj of objectsStream) {
      await minioClient.removeObject(bucketName, obj.name);
    }

    await minioClient.removeBucket(bucketName);
    console.log("All files deleted successfully");
    res.status(200).send("All files deleted successfully");
  } catch (error) {
    console.error("Error deleting all files:", error);
    res.status(500).send("Error deleting all files");
  } finally {
    isDeletingAll = false;
  }
});

// Endpoint to list files
app.get("/api/uploadfiles", async (req, res) => {
  const bucketExists = await minioClient.bucketExists(bucketName);
  if (!bucketExists) {
    return res.status(200).send("No Files to Download :(");
  }

  try {
    let fileSet = new Set();
    const objectsStream = minioClient.listObjects(bucketName, "", true);
    for await (const obj of objectsStream) {
      const baseFileName = obj.name.split(".").slice(0, -1).join(".");
      fileSet.add(baseFileName);
    }
    const fileListHtml = Array.from(fileSet)
      .map(
        (fileName) =>
          `<div class="file-item">${fileName}<button class="download-button" data-filename="${fileName}">Download</button></div>`
      )
      .join("");
    res.send(fileListHtml);
  } catch (error) {
    console.error("Error listing files:", error);
    res.status(500).send("Error listing files");
  }
});

// Endpoint to list files
app.get("/api/upload", async (req, res) => {
  const bucketExists = await minioClient.bucketExists(bucketName);
  if (!bucketExists) {
    return res.status(200).send("No Files Uploaded Yet!");
  }

  try {
    let fileSet = new Set();
    const objectsStream = minioClient.listObjects(bucketName, "", true);
    for await (const obj of objectsStream) {
      const baseFileName = obj.name.split(".").slice(0, -1).join(".");
      fileSet.add(baseFileName);
    }
    const fileListHtml = Array.from(fileSet)
      .map(
        (fileName) =>
          `<div class="file-item">${fileName}<button class="delete-button" data-filename="${fileName}">Delete</button></div>`
      )
      .join("");
    res.send(fileListHtml);
  } catch (error) {
    console.error("Error listing files:", error);
    res.status(500).send("Error listing files");
  }
});

// Endpoint to download a chunk of a file
app.get("/api/upload/download/:fileName/:chunkIndex", async (req, res) => {
  const fileName = req.params.fileName;
  const chunkIndex = req.params.chunkIndex;
  const chunkName = `${fileName}.${chunkIndex}`;

  try {
    const dataStream = await minioClient.getObject(bucketName, chunkName);
    dataStream.pipe(res);
  } catch (error) {
    console.error("Error downloading chunk:", error);
    res.status(500).send("Error downloading chunk");
  }
});

// Endpoint to get file metadata
app.get("/api/upload/metadata/:fileName", async (req, res) => {
  const fileName = req.params.fileName;
  try {
    let chunkCount = 0;
    const objectsStream = minioClient.listObjects(bucketName, fileName, true);
    for await (const obj of objectsStream) {
      if (obj.name.startsWith(fileName)) {
        chunkCount++;
      }
    }
    res.json({ chunkCount });
  } catch (error) {
    console.error("Error retrieving file metadata:", error);
    res.status(500).send("Error retrieving file metadata");
  }
});

// Endpoint to delete a file
app.delete("/api/upload/delete/:fileName", async (req, res) => {
  const fileName = req.params.fileName;

  try {
    const objectsStream = minioClient.listObjects(bucketName, fileName, true);
    for await (const obj of objectsStream) {
      if (obj.name.startsWith(fileName)) {
        await minioClient.removeObject(bucketName, obj.name);
      }
    }
    res.status(200).send("File deleted successfully");
  } catch (error) {
    console.error("Error deleting file:", error);
    res.status(500).send("Error deleting file");
  }
});

// Endpoint to upload a file
app.post("/api/upload", async (req, res) => {
  const fileName = req.headers["x-file-name"]
    ? Buffer.from(req.headers["x-file-name"], "base64").toString()
    : "default.bin";
  const chunkIndex = parseInt(req.headers["x-chunk-index"], 10);
  const chunkName = `${fileName}.${chunkIndex}`;

  const uploadStatus = ongoingUploads.get(fileName) || {
    uploadedChunks: new Set(),
    pendingChunks: new Set(),
    canceled: false,
  };
  if (uploadStatus.canceled) {
    return res.status(400).send("Upload canceled");
  }

  uploadStatus.pendingChunks.add(chunkIndex);
  ongoingUploads.set(fileName, uploadStatus);

  try {
    await minioClient.putObject(bucketName, chunkName, req.body);
    uploadStatus.uploadedChunks.add(chunkIndex);
    uploadStatus.pendingChunks.delete(chunkIndex);
    res.status(200).send("Chunk uploaded successfully");
  } catch (error) {
    uploadStatus.pendingChunks.delete(chunkIndex);
    console.error("Error uploading chunk:", error);
    res.status(500).send("Error uploading chunk");
  }
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
