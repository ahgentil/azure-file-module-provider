import {
  FileTypes,
  Logger,
} from "@medusajs/framework/types"
import {
  AbstractFileProviderService,
  MedusaError,
} from "@medusajs/framework/utils"
import {
  BlobServiceClient,
  ContainerClient,
} from "@azure/storage-blob"
import { v1 as uuidv1 } from "uuid"
import path from "path"
import { Readable } from "stream"

type InjectedDependencies = {
  logger: Logger,
}

type AzureBlobStorageServiceOptions = {
  containerName: string,
  connectionString: string,
}

class AzureBlobStorageFileProviderService extends AbstractFileProviderService {
  static identifier = "azure-file"

  protected logger_: Logger
  protected containerClient_: ContainerClient

  constructor({ logger }: InjectedDependencies, options: AzureBlobStorageServiceOptions) {
    super()

    this.logger_ = logger
    this.containerClient_ = BlobServiceClient
      .fromConnectionString(options.connectionString)
      .getContainerClient(options.containerName)
  }

  static validateOptions(options: AzureBlobStorageServiceOptions) {
    if (!options.containerName) {
      throw new MedusaError(
        MedusaError.Types.INVALID_DATA,
        "Blob container name required in the provider's options."
      )
    }
    if (!options.connectionString) {
      throw new MedusaError(
        MedusaError.Types.INVALID_DATA,
        "Connection string is required in the provider's options."
      )
    }
  }

  async upload(file: FileTypes.ProviderUploadFileDTO): Promise<FileTypes.ProviderFileResultDTO> {
    const parsedFilename = path.parse(file.filename)
    const fileKey = `${parsedFilename.name}-${uuidv1()}${parsedFilename.ext}`

    const blockBlobClient = this.containerClient_.getBlockBlobClient(fileKey)
    const buffer = Buffer.from(file.content, 'binary')

    try {
      await blockBlobClient.uploadStream(
        Readable.from(buffer),
        undefined, // bufferSize (optional)
        undefined, // maxConcurrency (optional)
        {
          blobHTTPHeaders: {
            blobContentType: file.mimeType
          }
        }
      )
    } catch (e) {
      this.logger_.error(e)
      throw e
    }

    return {
      url: blockBlobClient.url,
      key: fileKey,
    }
  }

  async delete(file: FileTypes.ProviderDeleteFileDTO): Promise<void> {
    const blockBlobClient = this.containerClient_.getBlockBlobClient(file.fileKey)

    try {
      await blockBlobClient.delete()
    } catch (e) {
      this.logger_.error(e)
      throw e
    }
  }

  async getAsBuffer(fileData: FileTypes.ProviderGetFileDTO): Promise<Buffer> {
    const blockBlobClient = this.containerClient_.getBlockBlobClient(fileData.fileKey)

    try {
      return blockBlobClient.downloadToBuffer()
    } catch (e) {
      this.logger_.error(e)
      throw e
    }
  }

  async getDownloadStream(fileData: FileTypes.ProviderGetFileDTO): Promise<Readable> {
    const blockBlobClient = this.containerClient_.getBlockBlobClient(fileData.fileKey)

    try {
      const downloadBlockBlobResponse = await blockBlobClient.download()
      return downloadBlockBlobResponse.readableStreamBody as Readable
    } catch (e) {
      this.logger_.error(e)
      throw e
    }
  }

  async getPresignedDownloadUrl(fileData: FileTypes.ProviderGetFileDTO): Promise<string> {
    const blockBlobClient = this.containerClient_.getBlockBlobClient(fileData.fileKey)

    return blockBlobClient.url
  }
}

export default AzureBlobStorageFileProviderService