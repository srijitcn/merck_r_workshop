# Databricks notebook source
library(dplyr)
library(ggplot2)
library(sparklyr)
library(shiny)

# COMMAND ----------

sc <- spark_connect(method = "databricks")
diamonds_tbl <- spark_read_csv(sc, path = "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")

# Define the UI
ui <- fluidPage(
  sliderInput("carat", "Select Carat Range:",
              min = 0, max = 5, value = c(0, 5), step = 0.01),
  plotOutput('plot')
)

# Define the server code
server <- function(input, output) {
  output$plot <- renderPlot({
    # Select diamonds in carat range
    df <- diamonds_tbl %>%
      select("carat", "price") %>%
      filter(carat >= !!input$carat[[1]], carat <= !!input$carat[[2]])
    
    # Scatter plot with smoothed means
    ggplot(df, aes(carat, price)) +
      geom_point(alpha = 1/2) +
      geom_smooth() +
      scale_size_area(max_size = 2) +
      ggtitle("Price vs. Carat")
  })
}

# Return a Shiny app object
options(shiny.port = 6666)
shinyApp(ui = ui, server = server)

# COMMAND ----------


