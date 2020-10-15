#' List Commits Where Data Changed
#'
#' Lists commits where the file in `path` was changed.
#'
#' @inheritParams list_data
#' @param path Path to the data file (or another file in the current repo). If
#'   no file extension is provided, `.tsv` is added.
#' @inheritDotParams git2r::commits
#'
#' @return A data frame with information about the commits that changed the
#'   repository or a file in the repository. Contains columns `sha`, `message`,
#'   `summary`, `author_name`, `author_time`, and others.
#'
#' @examples
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
#' cat("Unrelated changes", file = "README.md")
#' git2r::add(path = "README.md")
#' git2r::commit(message = "Second commit (non-data)")
#'
#' x <- rbind(data.frame(x = 6:10, y = runif(5)))
#' write_vc(x, "example", stage = TRUE)
#' git2r::add(path = "example*")
#' git2r::commit(message = "Third commit")
#'
#' list_data_changes()[c("sha", "message", "author_time")]
#' list_data_changes(path  = "example")[c("sha", "message", "author_time")]
#'
#' # clean up
#' setwd(owd)
#' unlink(tmpdir, recursive = TRUE)
#'
#' @export
list_data_changes <- function(root = ".", path = NULL, ...) {
  UseMethod("list_data_changes", root)
}

#' @export
list_data_changes.default <- function(root, path, ...) {
  stop("a 'root' of class ", class(root), " is not supported", call. = FALSE)
}

#' @export
list_data_changes.character <- function(root = ".", path = NULL, ...) {
  root <- git2r::repository(root, discover = FALSE)
  list_data_changes(root, path = path, ...)
}

#' @export
list_data_changes.git_repository <- function(root, path, ...) {
  if (!is.null(path) && path %in% list_data(root)) {
    path <- clean_data_path(root = git2r::workdir(root), file = path)["raw_file"]
  }

  commits <- git2r::commits(repo = root, path = path, ...)

  x <- lapply(commits, function(x) data.frame(
    sha = x$sha,
    message = x$message,
    summary = x$summary,
    author_name = x$author$name,
    author_email = x$author$email,
    author_time = as_local_time(x$author$when),
    committer_name = if (!is.null(x$committer)) x$committer$name else NA_character_,
    committer_email = if (!is.null(x$committer)) x$committer$email else NA_character_,
    committer_time = if (!is.null(x$committer)) as_local_time(x$committer$when) else NA_character_,
    stringsAsFactors = FALSE
  ))

  do.call("rbind", x)
}

as_local_time <- function(git_time, tz_local = Sys.timezone()) {
  x <- as.POSIXct(git_time)
  if (!is.null(tz_local)) {
    attributes(x)$tzone <- tz_local
  }
  x
}
