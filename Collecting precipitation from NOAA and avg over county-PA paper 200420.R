### Required packages
install.packages('rnoaa')
install.packages('dplyr')
install.packages('purrr')
install.packages('countyweather')

library(rnoaa)
library(dplyr)
library(purrr)
library(countyweather)

#NOAA Personal Key (Obtained from NOAA)
options(noaakey="zDEbMgoVIKDLFfLAxsTRQxbndDOCDySP")

###any(grepl("^\\.Renviron", list.files("~", all.files = TRUE)))
###options("noaakey" = Sys.getenv("noaakey"))

### Functions

###daily_stations
daily_stations <- function(fips, date_min = NULL, date_max = NULL) {
  
  FIPS <- paste0('FIPS:', fips)
  station_ids <- rnoaa::ncdc_stations(datasetid = 'GHCND', locationid = FIPS,
                                      limit = 10)
  
  station_df <- station_ids$data
  if (station_ids$meta$totalCount > 10) {
    how_many_more <- station_ids$meta$totalCount - 10
    more_stations <- rnoaa::ncdc_stations(datasetid = 'GHCND',
                                          locationid = FIPS,
                                          limit = how_many_more,
                                          offset = 10 + 1)
    station_df <- rbind(station_df, more_stations$data)
  }
  
  # If either `min_date` or `max_date` option was null, set to a date that
  # will keep all monitors in the filtering.
  if (is.null(date_max)) {
    date_max <- min(station_df$maxdate)
  }
  if (is.null(date_min)) {
    date_min <- max(station_df$mindate)
  }
  
  date_max <- lubridate::ymd(date_max)
  date_min <- lubridate::ymd(date_min)
  
  tot_df <- dplyr::mutate_(station_df,
                           mindate = ~ lubridate::ymd(mindate),
                           maxdate = ~ lubridate::ymd(maxdate)) %>%
    dplyr::filter_(~ maxdate >= date_min & mindate <= date_max) %>%
    dplyr::select_(.dots = c("id", "latitude", "longitude", "name")) %>%
    dplyr::mutate_(id = ~ gsub("GHCND:", "", id))
  
  return(tot_df)
}

### daily_df

daily_df <- function(stations, coverage = NULL, var = "all", date_min = NULL,
                     date_max = NULL, average_data = TRUE) {
  
  # get tidy full dataset for all monitors
  quiet_pull_monitors <- purrr::quietly(rnoaa::meteo_pull_monitors)
  
  if (length(var) == 1) {
    if (var == "all") {
      meteo_var <- "all"
    } 
    else {
      meteo_var <- toupper(var)
    }
  } else {
    meteo_var <- toupper(var)
  }
  
  meteo_df <- quiet_pull_monitors(monitors = stations$id,
                                  keep_flags = FALSE,
                                  date_min = date_min,
                                  date_max = date_max,
                                  var = meteo_var)$result
  
  # calculate coverage for each weather variable
  coverage_df <- rnoaa::meteo_coverage(meteo_df, verbose = FALSE)
  
  # filter station dataset based on specified coverage
  filtered <- filter_coverage(coverage_df,
                              coverage = coverage)
  good_monitors <- unique(filtered$id)
  
  # filter weather dataset based on stations with specified coverage
  filtered_data <- dplyr::filter_(meteo_df, ~ id %in% good_monitors)
  
  # steps to filter out erroneous data from individual stations
  # precipitation
  if ("prcp" %in% var) {
    filtered_data$prcp <- filtered_data$prcp / 10
    if (max(filtered_data$prcp, na.rm = TRUE) > 1100) {
      bad_prcp <- which(with(filtered_data, prcp > 1100))
      filtered_data <- filtered_data[-bad_prcp,]
    }
  }
  
  # snowfall
  if ("snow" %in% var) {
    if(max(filtered_data$snow, na.rm = TRUE) > 1600) {
      bad_snow <- which(with(filtered_data, snow > 1600))
      filtered_data <- filtered_data[-bad_snow,]
    }
  }
  
  # snow depth
  if ("snwd" %in% var) {
    if (max(filtered_data$snwd, na.rm = TRUE) > 11500) {
      bad_snwd <- which(with(filtered_data, snwd > 11500))
      filtered_data <- filtered_data[-bad_snwd,]
    }
  }
  
  # tmax
  if ("tmax" %in% var) {
    filtered_data$tmax <- filtered_data$tmax / 10
    if (max(filtered_data$tmax, na.rm = TRUE) > 57) {
      bad_tmax <- which(with(filtered_data, tmax > 57))
      filtered_data <- filtered_data[-bad_tmax,]
    }
  }
  
  # tmin
  if ("tmin" %in% var) {
    filtered_data$tmin <- filtered_data$tmin / 10
    if (min(filtered_data$tmin, na.rm = TRUE) < -62) {
      bad_tmin <- which(with(filtered_data, tmin < -62))
      filtered_data <- filtered_data[-bad_tmin,]
    }
  }
  
  all_cols <- colnames(filtered_data)
  not_vars <- c("id", "date")
  g_cols <- all_cols[!all_cols %in% not_vars]
  
  group_cols <- c("id", "key")
  
  stats <- filtered_data %>%
    dplyr::select_(quote(-date)) %>%
    tidyr::gather_(key_col = "key", value_col = "value", gather_cols = g_cols) %>%
    dplyr::group_by_(.dots = group_cols) %>%
    dplyr::summarize_(standard_dev = ~ sd(value, na.rm = TRUE),
                      min = ~ min(value, na.rm = TRUE),
                      max = ~ max(value, na.rm = TRUE),
                      range = ~ max - min)
  
  filtered <- dplyr::filter_(filtered, ~ id %in% good_monitors)
  stats <- dplyr::full_join(stats, filtered, by = c("id", "key"))
  
  stations <- dplyr::filter_(stations, ~ id %in% good_monitors)
  
  stations <- dplyr::full_join(stats, stations, by = "id") %>%
    dplyr::select_(quote(id), quote(name), quote(key), quote(latitude),
                   quote(longitude), quote(calc_coverage), quote(standard_dev),
                   quote(min), quote(max), quote(range))
  
  colnames(stations)[3] <- "var"
  
  filtered_data <- ave_daily(filtered_data)
  
  out <- list("daily_data" = filtered_data, "station_df" = stations)
  
  return(out)
  
}

### filter_coverage

filter_coverage <- function(coverage_df, coverage = 0) {
  
  if (is.null(coverage)) {
    coverage <- 0
  }
### changing coverage_df to coverage_df$summary  
  all_cols <- colnames(coverage_df$summary)
  not_vars <- c("id", "start_date", "end_date", "total_obs")
  g_cols <- all_cols[!all_cols %in% not_vars]

### changing coverage_df to coverage_df$summary  
  
  filtered <- dplyr::select_(coverage_df$summary,
                             .dots = list("-start_date", "-end_date",
                                          "-total_obs")) %>%
    tidyr::gather_(key_col = "key", value_col = "covered",
                   gather_cols = g_cols)  %>%
    dplyr::filter_(~ covered >= coverage) %>%
    dplyr::mutate_(covered_n = ~ 1) %>%
    dplyr::group_by_(.dots = list("id")) %>%
    dplyr::mutate_(good_monitor = ~ sum(!is.na(covered_n)) > 0) %>%
    dplyr::ungroup() %>%
    dplyr::filter_(~ good_monitor) %>%
    dplyr::select_(.dots = list("-good_monitor", "-covered_n"))
  
  colnames(filtered)[3] <- "calc_coverage"
  
  return(filtered)
}

### ave_daily

ave_daily <- function(weather_data) {
  
  all_cols <- colnames(weather_data)
  not_vars <- c("id", "date")
  g_cols <- all_cols[!all_cols %in% not_vars]
  
  #not sure about -id -date cols - how to do NSE here
  if (average_data == TRUE) {
    
  averaged_data <- tidyr::gather_(weather_data, key_col = "key",
                                  value_col = "value",
                                  gather_cols = g_cols) %>%
    dplyr::group_by_(.dots = c("date", "key")) %>%
    dplyr::summarize_(mean = ~ mean(value, na.rm = TRUE)) %>%
    tidyr::spread_(key_col = "key", value_col = "mean") %>%
    dplyr::ungroup()
  }
  
  if (max_data == TRUE) {
    averaged_data <- tidyr::gather_(weather_data, key_col = "key",
                                    value_col = "value",
                                    gather_cols = g_cols) %>%
      dplyr::group_by_(.dots = c("date", "key")) %>%
      dplyr::summarize_(max = ~ max(value, na.rm = TRUE)) %>%
      tidyr::spread_(key_col = "key", value_col = "max") %>%
      dplyr::ungroup()
  }
  
  if (average_data == FALSE & max_data == FALSE) {
    return(filtered_data)
  }
    
  n_reporting <- tidyr::gather_(weather_data, key_col = "key",
                                value_col = "value",
                                gather_cols = g_cols) %>%
    dplyr::group_by_(.dots = c("date", "key")) %>%
    dplyr::summarize_(n_reporting = ~ sum(!is.na(value))) %>%
    dplyr::mutate_(key = ~ paste(key, "reporting", sep = "_")) %>%
    tidyr::spread_(key_col = "key", value_col = "n_reporting")
  
  averaged_data <- dplyr::left_join(averaged_data, n_reporting,
                                    by = "date")
  return(averaged_data)
}


### daily_stationmap

daily_stationmap <- function(fips, daily_data, point_color = "firebrick",
                             point_size = 2, station_label = FALSE) {
  
  # for plot title
  census_data <- countyweather::county_centers
  row_num <- which(grepl(fips, census_data$fips))
  title <- as.character(census_data[row_num, "name"])
  
  # for ggmap lat/lon
  loc_fips <- which(census_data$fips == fips)
  lat_fips <- as.numeric(census_data[loc_fips, "latitude"])
  lon_fips <- as.numeric(census_data[loc_fips, "longitude"])
  
  state <- stringi::stri_sub(fips, 1, 2)
  county <- stringi::stri_sub(fips, 3)
  
  suppressMessages(
    shp <- tigris::counties(state, cb = TRUE)
  )
  county_shp <- shp[shp$COUNTYFP == county, ]
  
  # convert to raster so that we can add geom_raster() (which gets rid of the
  # geom_polygons island problem)
  r <- raster::raster(raster::extent(county_shp))
  raster::res(r) <- 0.001
  raster::projection(r) <- sp::proj4string(county_shp)
  r <- raster::rasterize(county_shp, r)
  rdf <- data.frame(raster::rasterToPoints(r))
  
  # use range of raster object to figure out what zoom to use in ggmap
  x_range <- r@extent[2] - r@extent[1]
  y_range <- r@extent[4] - r@extent[3]
  
  # limits were calculated by finding out the x and y limits of a ggmap at each
  # zoom, then accounting for the extra space we want to add around county
  # shapes.
  
  if (x_range > y_range) {
    if (x_range <= 0.1997) {
      
      zoom <- 12
      
      xmin <- r@extent[1] - 0.01
      xmax <- r@extent[2] + 0.01
      ymin <- r@extent[3] - 0.01
      ymax <- r@extent[4] + 0.01
    }
    
    if (x_range <= 0.3894 & x_range > 0.1997) {
      
      zoom <- 11
      
      xmin <- r@extent[1] - 0.025
      xmax <- r@extent[2] + 0.025
      ymin <- r@extent[3] - 0.025
      ymax <- r@extent[4] + 0.025
    }
    
    if(x_range <= 0.7989 & x_range > 0.3894) {
      
      zoom <- 10
      
      xmin <- r@extent[1] - 0.04
      xmax <- r@extent[2] + 0.04
      ymin <- r@extent[3] - 0.04
      ymax <- r@extent[4] + 0.04
    }
    
    if (x_range <= 1.6378 & x_range > 0.7989) {
      
      zoom <- 9
      
      xmin <- r@extent[1] - 0.06
      xmax <- r@extent[2] + 0.06
      ymin <- r@extent[3] - 0.06
      ymax <- r@extent[4] + 0.06
    }
    
    if (x_range <= 3.3556 & x_range > 1.6378) {
      
      zoom <- 8
      
      xmin <- r@extent[1] - 0.08
      xmax <- r@extent[2] + 0.08
      ymin <- r@extent[3] - 0.08
      ymax <- r@extent[4] + 0.08
    }
    
    if (x_range <= 6.8313 & x_range > 3.3556) {
      
      zoom <- 7
      
      xmin <- r@extent[1] - 0.1
      xmax <- r@extent[2] + 0.1
      ymin <- r@extent[3] - 0.1
      ymax <- r@extent[4] + 0.1
    }
    
  } else {
    if(y_range <= 0.1616) {
      
      zoom <- 12
      
      xmin <- r@extent[1] - 0.01
      xmax <- r@extent[2] + 0.01
      ymin <- r@extent[3] - 0.01
      ymax <- r@extent[4] + 0.01
    }
    
    if (y_range <= 0.3135 & y_range > 0.1616) {
      
      zoom <- 11
      
      xmin <- r@extent[1] - 0.025
      xmax <- r@extent[2] + 0.025
      ymin <- r@extent[3] - 0.025
      ymax <- r@extent[4] + 0.025
    }
    
    if (y_range <= 0.647 & y_range > 0.3135) {
      
      zoom <- 10
      
      xmin <- r@extent[1] - 0.04
      xmax <- r@extent[2] + 0.04
      ymin <- r@extent[3] - 0.04
      ymax <- r@extent[4] + 0.04
    }
    
    if (y_range <= 1.3302 & y_range > 0.647) {
      
      zoom <- 9
      
      xmin <- r@extent[1] - 0.06
      xmax <- r@extent[2] + 0.06
      ymin <- r@extent[3] - 0.06
      ymax <- r@extent[4] + 0.06
    }
    
    if (y_range <= 2.7478 & y_range > 1.3302) {
      
      zoom <- 8
      
      xmin <- r@extent[1] - 0.08
      xmax <- r@extent[2] + 0.08
      ymin <- r@extent[3] - 0.08
      ymax <- r@extent[4] + 0.08
    }
    
    if (y_range <= 2.8313 & y_range > 2.7478) {
      
      zoom <- 7
      
      xmin <- r@extent[1] - 0.1
      xmax <- r@extent[2] + 0.1
      ymin <- r@extent[3] - 0.1
      ymax <- r@extent[4] + 0.1
    }
  }
  
  county <- suppressMessages(ggmap::get_map(c(lon_fips,
                                              lat_fips), zoom = zoom,
                                            color = "bw"))
  
  gg_map <- ggmap::ggmap(county)
  
  # limits of a ggmap depend on your center lat/lon (this means the limits
  # above won't work exactly for every county)
  map_ymin <- gg_map$data$lat[1]
  map_ymax <- gg_map$data$lat[3]
  map_xmin <- gg_map$data$lon[1]
  map_xmax <- gg_map$data$lon[2]
  
  if ((ymin < map_ymin) | (ymax > map_ymax) | (xmin < map_xmin) |
      (xmax > map_xmax)) {
    zoom <- zoom - 1
    county <- suppressMessages(ggmap::get_map(c(lon_fips, lat_fips),
                                              zoom = zoom, color = "bw"))
    gg_map <- ggmap::ggmap(county)
  }
  
  map <- gg_map +
    ggplot2::coord_fixed(xlim = c(xmin, xmax),
                         ylim = c(ymin, ymax)) +
    ggplot2::geom_raster(mapping = ggplot2::aes_(~x, ~y),
                         data = rdf, fill = "yellow",
                         alpha = 0.2,
                         inherit.aes = FALSE,
                         na.rm = TRUE)
  
  station_df <- daily_data$station_df %>%
    dplyr::tbl_df() %>%
    dplyr::filter_(~ !duplicated(id)) %>%
    dplyr::arrange_(~ dplyr::desc(latitude))
  
  name_levels <- unique(station_df$name)
  
  station_df <- station_df %>%
    dplyr::mutate_(name = ~ factor(name, levels = name_levels))
  
  if (station_label == TRUE) {
    map_out <- map +
      ggplot2::geom_point(data = station_df,
                          ggplot2::aes_(~ longitude, ~ latitude,
                                        fill = ~ name),
                          colour = "black",
                          size = point_size,
                          shape = 21) +
      ggplot2::ggtitle(title) +
      ggplot2::theme_void()
  } else {
    map_out <- map +
      ggplot2::geom_point(data = station_df,
                          ggplot2::aes_(~ longitude, ~ latitude),
                          colour = point_color,
                          size = point_size) +
      ggplot2::theme_void() +
      ggplot2::ggtitle(title)
  }
  
  return(map_out)
  
}
#######################################################

# Import PA Paper Dataset
pa_dataset <- read.csv(file.choose(),header = TRUE)

for (i in 1:nrow(pa_dataset)) {

### skip counties with no precipitation records
  if(any(i == c(9,47,97,130,155,291,481,492,497))) next
  #if(any(i == c(225))) next
### Analaysis Progress
  print(paste("Progress: ",i,"/",nrow(pa_dataset),sep = ""))

###Inputs
###fips = "12086"
  ###fips = "55103"
fips = pa_dataset[i,"County_FIPS"]
###date_min = "1992-08-01"
###date_min = "2016-09-21"
date_min = substr(pa_dataset[i,"incidentBeginDate"],1,10)
###date_max = "1992-08-31"
###date_max = "2016-09-22"
date_max = substr(pa_dataset[i,"incidentEndDate"],1,10)
var="prcp"
coverage = NULL

#For average over stations set average_data = TRUE and max_data =FALSE, and vice versa for maximum over stations. For neither, set both to FALSE
average_data = FALSE
max_data =TRUE

#Check
if ((average_data==TRUE & max_data==TRUE) | (average_data==FALSE & max_data==FALSE)) stop("Set either average_data or max_data as TRUE")

station_label = FALSE
verbose = TRUE

census_data <- countyweather::county_centers
loc_fips <- which(census_data$fips == fips)

if (verbose) {
  
  message(paste0("Getting daily weather data for ",
                 census_data[loc_fips, "name"], ".",
                 " This may take a while."))
  
}

stations <- daily_stations(fips = fips, date_min = date_min,
                           date_max = date_max)

weather_data <- daily_df(stations = stations,
                         var = var,
                         date_min = date_min,
                         date_max = date_max,
                         coverage = coverage,
                         average_data = average_data)

###Need to fix maping function later but I do not need that feature for now

###station_map <- daily_stationmap(fips = fips,
###                                daily_data = weather_data,
###                                station_label = station_label)

###list <- list("daily_data" = weather_data$daily_data,
###             "station_metadata" = weather_data$station_df,
###             "station_map" = station_map)

daily_fips <- list("daily_data" = weather_data$daily_data,
             "station_metadata" = weather_data$station_df)



### max 3-day precipitation
max_3d_prcp <- 0

if (nrow(daily_fips$daily_data) >= 3) {
  for (j in 1:(nrow(daily_fips$daily_data)-2)) {
  avg_prcp <- sum(daily_fips$daily_data[j:(j+2),"prcp"],na.rm = TRUE)
  if (avg_prcp > max_3d_prcp) max_3d_prcp <-avg_prcp
  }
} else max_3d_prcp <- sum(daily_fips$daily_data[,"prcp"],na.rm = TRUE)

pa_dataset[i,"max_3d_prcp"] <- max_3d_prcp

### max 5-day precipitation
max_5d_prcp <- 0

if (nrow(daily_fips$daily_data) >= 5) {
  for (j in 1:(nrow(daily_fips$daily_data)-4)) {
  avg_prcp <- sum(daily_fips$daily_data[j:(j+4),"prcp"],na.rm = TRUE)
  if (avg_prcp > max_5d_prcp) max_5d_prcp <-avg_prcp
  }
} else max_5d_prcp <- sum(daily_fips$daily_data[,"prcp"],na.rm = TRUE)

pa_dataset[i,"max_5d_prcp"] <- max_5d_prcp

### count of days
pa_dataset[i,"prcp_days"] <- nrow(daily_fips$daily_data)

### count of days with no precipitation observation available
pa_dataset[i,"prcp_NAs"] <- sum(is.na(daily_fips$daily_data$prcp))

  
}

###Rename the new covariates to represent the summary method (i.e. average or maximum)
if (average_data == TRUE) {
  colnames(pa_dataset)[colnames(pa_dataset) == 'max_3d_prcp'] <- 'max_3d_avg_prcp'
  colnames(pa_dataset)[colnames(pa_dataset) == 'max_5d_prcp'] <- 'max_5d_avg_prcp'
  
}

if (max_data == TRUE) {
  colnames(pa_dataset)[colnames(pa_dataset) == 'max_3d_prcp'] <- 'max_3d_max_prcp'
  colnames(pa_dataset)[colnames(pa_dataset) == 'max_5d_prcp'] <- 'max_5d_max_prcp'
  
}

###Output
write.csv(pa_dataset, file = "pa_dataset 200420.csv",row.names = FALSE)
