import AzureBlobStorageFileProviderService from "./service"
import { 
  ModuleProvider, 
  Modules
} from "@medusajs/framework/utils"

export default ModuleProvider(Modules.FILE, {
  services: [AzureBlobStorageFileProviderService],
})