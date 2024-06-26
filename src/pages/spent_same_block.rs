use crate::charts::{Chart, Color, Dataset, Kind};
use crate::pages::{to_label_map, Page};
use crate::process::{Stats, TxStats};

pub fn spent_same_block(stats: &Stats, tx_stats: &TxStats) -> Page {
    let mut charts = vec![];
    //let perc: Vec<_> = total.iter().zip(spent.iter()).map(|e| *e.1 / *e.0).collect();
    let (vec, mul) = tx_stats.total_outputs_per_period.finish();
    let labels: Vec<_> = to_label_map(&vec, mul).keys().cloned().collect();

    let mut chart = Chart::new("Spent in the same block".to_string(), Kind::Line, labels);

    let dataset = Dataset {
        label: "outputs".to_string(),
        data: tx_stats.total_outputs_per_period.finish().0,
        background_color: vec![Color::Orange],
        border_color: vec![Color::Orange],
        fill: true,
        ..Default::default()
    };
    chart.add_dataset(dataset, None);

    let dataset = Dataset {
        label: "spent in same block".to_string(),
        data: stats.total_spent_in_block_per_period.finish().0,
        background_color: vec![Color::Red],
        border_color: vec![Color::Red],
        fill: true,
        ..Default::default()
    };
    chart.add_dataset(dataset, None);

    /*
    let dataset = Dataset {
        label: "percentage".to_string(),
        data: perc,
        background_color: vec![Color::Blue],
        border_color: vec![Color::Blue],
        fill: true,
        borderDash: [5, 5],
        hidden: false,
    };
    chart.add_dataset(dataset);
    TODO add borderDash to dataset, use ..Default::default()
    add scales to option
    scales: {
         yAxes: [{
             type: 'linear',
             display: true,
             position: 'left',
             id: 'y-axis-1',
         }, {
             type: 'linear',
             display: true,
             position: 'right',
             id: 'y-axis-2',
             gridLines: {
                 drawOnChartArea: false,
             },
         }],
    }


     */

    charts.push(chart);

    Page {
        title: "Spent in the same block".to_string(),
        description:
            "Charts showing the number of output which are spent in the same block they are created"
                .to_string(),
        permalink: "spent-same-block".to_string(),
        charts,
        text: "".to_string(),
    }
}
