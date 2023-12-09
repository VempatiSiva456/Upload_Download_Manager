// upload related code

// globals

let isCanceled = false;
const MAX_CONCURRENT_UPLOADS = 5;
let activeUploads = 0;
let uploadQueue = [];
let uploadedChunks = 0;
let totalFileChunks = 0;
let isPaused = false;
let currentUpload = null;

let isRestarting = false;
let currentFileDetails = null;

// functions

function dataURLtoBlob(dataurl) {
  const arr = dataurl.split(",");
  const bstr = atob(arr[1]);
  let n = bstr.length;
  const u8arr = new Uint8Array(n);

  while (n--) {
    u8arr[n] = bstr.charCodeAt(n);
  }

  return new Blob([u8arr], { type: "application/octet-stream" });
}

function uploadChunk(
  chunkData,
  fileName,
  chunkIndex,
  totalChunks,
  fileSize,
  serverUrl,
  isCanceled
) {
  return new Promise(async (resolve, reject) => {
    const start = chunkIndex * chunkData.size;
    const end =
      start + chunkData.size < fileSize ? start + chunkData.size : fileSize;
    const headers = new Headers();
    headers.append("Content-Type", "application/octet-stream");
    headers.append("Content-Range", `bytes ${start}-${end - 1}/${fileSize}`);
    headers.append("X-File-Name", btoa(fileName));
    headers.append("X-Chunk-Index", chunkIndex);
    headers.append("X-Total-Chunks", totalChunks);
    headers.append("X-Is-Canceled", isCanceled.toString());

    try {
      const response = await fetch(serverUrl, {
        method: "POST",
        headers: headers,
        body: chunkData,
      });

      if (response.ok) {
        resolve(await response.text());
      } else {
        reject(new Error("Upload failed: " + response.statusText));
      }
    } catch (error) {
      reject(new Error("Network error"));
    }
  });
}

function handleFileUpload(fileUrl, fileName, fileSize) {
  const chunkSize = 3 * 1024 * 1024; // 3MB chunks
  totalFileChunks = Math.ceil(fileSize / chunkSize);

  fetch(fileUrl)
    .then((response) => response.blob())
    .then((blob) => {
      for (let i = 0; i < totalFileChunks; i++) {
        const start = i * chunkSize;
        const end = Math.min(start + chunkSize, fileSize);
        const chunkData = blob.slice(start, end);
        const serverUrl = "http://localhost:5000/api/upload";

        uploadQueue.push({
          chunkData,
          fileName,
          chunkIndex: i,
          totalChunks: totalFileChunks,
          fileSize,
          serverUrl,
          isCanceled,
        });
      }
      processQueue();
    });
}

function processQueue() {
  if (isPaused) {
    console.log("Upload paused");
    return; // Stop processing when paused
  }

  if (uploadQueue.length > 0) {
    // Show the progress bar when starting the upload process
    chrome.runtime.sendMessage({ type: "showProgressBar" });
  }

  while (activeUploads < MAX_CONCURRENT_UPLOADS && uploadQueue.length > 0) {
    const {
      chunkData,
      fileName,
      chunkIndex,
      totalChunks,
      fileSize,
      serverUrl,
      isCanceled,
    } = uploadQueue.shift();
    activeUploads++;
    console.log("Active Uploads", activeUploads);
    uploadChunk(
      chunkData,
      fileName,
      chunkIndex,
      totalChunks,
      fileSize,
      serverUrl,
      isCanceled
    )
      .then(() => {
        activeUploads--;
        uploadedChunks++;
        updateProgress();
        processQueue();

        // Check if this was the last chunk
        if (uploadedChunks === totalChunks) {
          // Hide the progress bar when upload is complete
          chrome.runtime.sendMessage({ type: "hideProgressBar" });
          chrome.runtime.sendMessage({ type: "refreshFileList" });
          chrome.runtime.sendMessage({ type: "uploadComplete" });
          notifyUploadCompletion("uploadComplete");
          resetUploadState();
        }
      })
      .catch((error) => {
        console.log("Upload Process Error: ", error);
        activeUploads--;
        processQueue();
      });
  }
}


function updateProgress() {
  const progress = (uploadedChunks / totalFileChunks) * 100;
  chrome.runtime.sendMessage({ type: "updateProgress", progress: progress });
}

// After an upload is complete or canceled, send a message to the popup script
function notifyUploadCompletion(status) {
  chrome.runtime.sendMessage({ type: status });
  currentUpload = null;
  isCanceled = false;
  resetUploadState();
}

function resetUploadState() {
  // Reset all the state variables
  uploadQueue = [];
  activeUploads = 0;
  uploadedChunks = 0;
  totalFileChunks = 0;
  isPaused = false;
  isCanceled = false;
}

// Message Listeners (Chrome)

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "restartUpload") {
    isRestarting = true;
    currentFileDetails = message.file;
    isCanceled = true; // Set the cancel flag

    // Clear the upload queue and reset active uploads
    uploadQueue = [];
    activeUploads = 0;

    // Hide the progress bar and reset progress
    chrome.runtime.sendMessage({ type: "hideProgressBar" });
    chrome.runtime.sendMessage({ type: "resetProgress" });

    // Call cancel on the server
    fetch(`http://localhost:5000/api/upload/cancel`, {
      method: "POST",
      headers: { "X-File-Name": btoa(currentFileDetails.name) },
    })
      .then((response) => {
        if (response.ok) {
          // Hide the progress bar and reset progress
          chrome.runtime.sendMessage({ type: "hideProgressBar" });
          chrome.runtime.sendMessage({ type: "resetProgress" });
          console.log("Canceled the uploading process successfully");
          if (isRestarting) {
            // Restart upload process after a brief delay to ensure cancellation is complete

            handleFileUpload(
              currentFileDetails.fileUrl,
              currentFileDetails.name,
              currentFileDetails.size
            );
            isRestarting = false;
          }
        }
      })
      .catch((error) =>
        console.error("Error during upload cancellation:", error)
      );
  }
});

// Message listener for pause/resume
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "pauseResumeUpload") {
    isPaused = !isPaused;
    console.log("Pause/Resume Toggled. Current state:", isPaused);
    if (!isPaused) {
      // If not paused, resume the upload process
      processQueue();
    }
  }
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "deleteAllFiles") {
    fetch(`http://localhost:5000/api/upload/deleteAll`, {
      method: "DELETE",
    })
      .then((response) => {
        if (response.ok) {
          console.log("All files deleted successfully");
          // refresh the file list in the popup
          chrome.runtime.sendMessage({ type: "refreshFileList" });
        }
      })
      .catch((error) => console.error("Error deleting all files:", error));
  }
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.type === "startUpload") {
    currentUpload = message;
    handleFileUpload(message.fileUrl, message.fileName, message.fileSize);
  } else if (message.type === "cancelCurrentUpload") {
    // Cancel only the current upload
    if (currentUpload && currentUpload.fileName === message.fileName) {
      isCanceled = true;
      console.log("clicked cancel");
      const fileName = uploadQueue.length > 0 ? uploadQueue[0].fileName : null;

      // Clear the upload queue and reset active uploads
      uploadQueue = [];
      activeUploads = 0;

      // Hide the progress bar and refresh the file list in the popup
      chrome.runtime.sendMessage({ type: "hideProgressBar" });
      chrome.runtime.sendMessage({ type: "refreshFileList" });

      if (fileName) {
        // Send a request to the server to cancel the upload
        fetch(`http://localhost:5000/api/upload/cancel`, {
          method: "POST",
          headers: { "X-File-Name": btoa(fileName) },
        })
          .then((response) => {
            if (response.ok) {
              console.log("Cancelled the Uploading process successfully");
              // refresh the file list in the popup
              chrome.runtime.sendMessage({ type: "refreshFileList" });
              chrome.runtime.sendMessage({ type: "uploadCancelled" });
              notifyUploadCompletion("uploadCancelled");
            }
          })
          .catch((error) => console.error("Cancellation error:", error));
      }
    }
  }
});

