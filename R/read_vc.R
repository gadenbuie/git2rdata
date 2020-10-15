#' Read a Git2rdata Object from Disk
#'
#' @description
#' `read_vc()` handles git2rdata objects stored by `write_vc()`. It reads and
#' verifies the metadata file (`.yml`). Then it reads and verifies the raw data.
#' The last step is back-transforming any transformation done by `meta()` to
#' return the `data.frame` as stored by `write_vc()`.
#'
#' `read_vc()` is an S3 generic on `root` which currently handles `"character"`
#' (a path) and `"git-repository"` (from `git2r`). S3 methods for other version
#' control system could be added.
#'
#' @inheritParams write_vc
#' @return The `data.frame` with the file names and hashes as attributes.
#' @rdname read_vc
#' @export
#' @family storage
#' @template example_io
read_vc <- function(file, root = ".") {
  UseMethod("read_vc", root)
}

#' @export
read_vc.default <- function(file, root) {
  stop("a 'root' of class ", class(root), " is not supported", call. = FALSE)
}

#' @export
#' @importFrom assertthat assert_that is.string has_name
#' @importFrom yaml read_yaml
#' @importFrom utils read.table
#' @importFrom stats setNames
#' @importFrom git2r hashfile
read_vc.character <- function(file, root = ".") {
  assert_that(is.string(file), is.string(root))
  root <- normalizePath(root, winslash = "/", mustWork = TRUE)

  file <- clean_data_path(root = root, file = file)
  tryCatch(
    is_git2rdata(file = remove_root(file = file["meta_file"], root = root),
                 root = root, message = "error"),
    error = function(e) {
      stop(e$message, call. = FALSE)
    }
  )
  assert_that(
    all(file.exists(file)),
    msg = "raw file and/or meta file missing"
  )

  # read the metadata
  meta_data <- read_yaml(file["meta_file"])
  optimize <- meta_data[["..generic"]][["optimize"]]
  col_type <- list(
    c(
      character = "character", factor = "character", integer = "integer",
      numeric = "numeric", logical = "logical", Date = "Date",
      POSIXct = "character", complex = "complex"
    ),
    c(
      character = "character", factor = "integer", integer = "integer",
      numeric = "numeric", logical = "integer", Date = "integer",
      POSIXct = "numeric", complex = "complex"
    )
  )[[optimize + 1]]
  na_string <- meta_data[["..generic"]][["NA string"]]
  details <- meta_data[names(meta_data) != "..generic"]
  col_names <- names(details)
  col_classes <- vapply(details, "[[", character(1), "class")

  # read the raw data and check the data hash
  raw_data <- read.table(
    file = file["raw_file"], header = TRUE, sep = "\t", quote = "\"",
    dec = ".", numerals = "warn.loss", na.strings = na_string,
    colClasses = setNames(col_type[col_classes], col_names), comment.char = "",
    stringsAsFactors = FALSE, fileEncoding = "UTF-8"
  )

  dh <- datahash(file["raw_file"])
  if (meta_data[["..generic"]][["data_hash"]] != dh) {
    meta_data[["..generic"]][["data_hash"]] <- dh
    warning("Mismatching data hash. Data altered outside of git2rdata.",
            call. = FALSE)
  }

  raw_data <- reinstate(
    raw_data = raw_data, col_names = col_names, col_classes = col_classes,
    details = details, optimize = optimize
  )

  names(file) <-
    c(
      meta_data[["..generic"]][["data_hash"]],
      meta_data[["..generic"]][["hash"]]
    )
  attr(raw_data, "source") <- file
  return(raw_data)
}

reinstate <- function(raw_data, col_names, col_classes, details, optimize) {
  # reinstate factors
  for (id in col_names[col_classes == "factor"]) {
    if (optimize) {
      raw_data[[id]] <- factor(
        raw_data[[id]],
        levels = details[[id]][["index"]],
        labels = details[[id]][["labels"]],
        ordered = details[[id]][["ordered"]]
      )
    } else {
      raw_data[[id]] <- factor(
        raw_data[[id]],
        levels = details[[id]][["labels"]],
        labels = details[[id]][["labels"]],
        ordered = details[[id]][["ordered"]]
      )
    }
  }

  # reinstate POSIXct
  for (id in col_names[col_classes == "POSIXct"]) {
    if (optimize) {
      raw_data[[id]] <- as.POSIXct(
        raw_data[[id]],
        origin = details[[id]][["origin"]],
        tz = details[[id]][["timezone"]]
      )
    } else {
      raw_data[[id]] <- as.POSIXct(
        raw_data[[id]],
        format = details[[id]][["format"]],
        tz = details[[id]][["timezone"]]
      )
    }
  }

  if (!optimize) {
    return(raw_data)
  }
  # reinstate logical
  for (id in col_names[col_classes == "logical"]) {
    raw_data[[id]] <- as.logical(raw_data[[id]])
  }

  # reinstage Date
  for (id in col_names[col_classes == "Date"]) {
    raw_data[[id]] <- as.Date(raw_data[[id]],
                              origin = details[[id]][["origin"]])
  }
  return(raw_data)
}

#' @export
#' @importFrom git2r workdir
#' @include write_vc.R
read_vc.git_repository <- function(file, root) {
  read_vc(file, root = workdir(root))
}

#' Locate Project Root at Git Reference
#'
#' Returns a `git_tree` object representing the project root at the given
#' reference, typically a branch name or a commit SHA. When combined with
#' [read_vc()], this function lets users load data at a particular point in time
#' (specifically at a given commit snapshot) without having to explicitly check
#' out the reference using `git` directly.
#'
#' @inheritParams read_vc
#' @param ref A `git` reference: a branch name, tag name, or commit hash (sha).
#'
#' @return A `git_tree` object to be used by `read_vc()` to read a `git2rdata`
#'   file at a particular point in time.
#'
#' @examples
#'
#' # Use a temporary directory for this example
#' dir.create(tmpdir <- tempfile())
#' owd <- setwd(tmpdir)
#'
#' # Create our first data commit
#' set.seed(42)
#' x <- data.frame(x = 1:5, y = runif(5))
#'
#' git2r::init()
#' write_vc(x, "example", sorting = "x")
#' git2r::add(path = "example*")
#' git2r::commit(message = "First commit")
#' git2r::tag(name = "v0.0.1")
#'
#' x <- rbind(data.frame(x = 6:10, y = runif(5)))
#' write_vc(x, "example", stage = TRUE)
#' git2r::add(path = "example*")
#' git2r::commit(message = "Second commit")
#'
#' root_at_ref(ref = "v0.0.1")
#' read_vc("example", root_at_ref(ref = "v0.0.1"))
#'
#' @export
root_at_ref <- function(root = ".", ref = "HEAD") {
  assert_that(is.string(ref))

  if (!inherits(root, "git_repository")) {
    root <- git2r::repository(root)
  }
  commit <- git2r::revparse_single(root, revision = ref)
  tree <- git2r::tree(commit)
  attributes(tree)$ref <- ref
  tree
}


#' @export
read_vc.git_tree <- function(file, root) {
  assert_that(is.string(file))
  path_file <- sub("[.](tsv|yml)$", "", basename(file))
  path_dir <- dirname(file)
  path_parts <- strsplit(dirname(file), "^(?=/)(?!//)|(?<!^)(?<!^/)/", perl = TRUE)[[1]]

  ref <- attributes(root)$ref
  if (path_parts != ".") {
    for (path_part in path_parts) {
      root <- root[path_part]
    }
  }

  x <- list()
  for (type in c(".tsv", ".yml")) {
    path_file_type <- paste0(path_file, type)
    if (!path_file_type %in% root$name) {
      stop(
        path_dir, "/", path_file_type,
        " not found at ref: ", ref
      )
    }
    x[[c(".tsv" = "data", ".yml" = "meta")[type]]] <- root[path_file_type]
  }

  tmpdir <- tempfile()
  dir.create(tmpdir)

  tmp_data <- file.path(tmpdir, paste0(path_file, ".tsv"))
  tmp_meta <- file.path(tmpdir, paste0(path_file, ".yml"))
  on.exit(unlink(tmpdir, recursive = TRUE))

  data <- git2r::content(x$data, split = TRUE)
  writeLines(data, con = tmp_data)

  meta <- git2r::content(x$meta, split = TRUE)
  writeLines(meta, con = tmp_meta)

  read_vc(path_file, root = dirname(tmp_data))
}
