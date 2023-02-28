### Get tag and increment package version
ref_name <- Sys.getenv("GITHUB_REF_NAME")

semver_regex <- "^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$"
if(!grepl(pattern = semver_regex, ref_name)) {
  stop("Invalid semantic version.")
}

desc <- readLines("./DESCRIPTION")
desc2 <- gsub(pattern = "Version:.*", replace = paste0("Version: ", ref_name), x = desc)
desc3 <- gsub(pattern = "Date:.*", replace = paste0("Date: ", Sys.Date()), x = desc2)
writeLines(desc3, con = "./DESCRIPTION")

### Build package
pkg_path <- pkgbuild::build(path = ".", dest_path = ".")
message(pkg_path)

### Upload to Tercen

## Tercen settings
serviceUri <- Sys.getenv("TERCEN_SERVICE_URI")
username <- Sys.getenv("TERCEN_GITHUB_USER_USERNAME")
pwd <- Sys.getenv("TERCEN_GITHUB_USER_PASSWORD")
teamName <- Sys.getenv("TERCEN_CRAN_TEAM")
projectName <- Sys.getenv("TERCEN_CRAN_PROJECT")

library(tercen)
library(tercenApi)

client = TercenClient$new(
  serviceUri = serviceUri,
  username = username,
  password = pwd
)

projects = client$documentService$findProjectByOwnersAndName(
  startKey = list(teamName, projectName),
  endKey = list(teamName, projectName)
)

project = Find(function(p) identical(p$name, projectName), projects)
if(is.null(project)) stop("Project not found.")

files <- client$projectDocumentService$findFileByLastModifiedDate(
  startKey = list(project$id, '2042'),
  endKey = list(project$id, ''),
  useFactory = TRUE
)

file_stats = Find(function(p) identical(p$name, basename(pkg_path)), files)
if(!is.null(file_stats)) stop("Package build for this version already exists.")

fileDoc = FileDocument$new()
fileDoc$name = basename(pkg_path)
fileDoc$projectId = project$id
fileDoc$acl$owner = project$acl$owner
fileDoc$isPublic = FALSE

bytes = readBin(pkg_path, "raw", n = 10e6)
fileDoc = client$fileService$upload(fileDoc, bytes)