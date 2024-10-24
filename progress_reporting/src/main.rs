use core::time;
use std::thread::sleep;

use progress_reporting::DataProgressReporter;

fn main() {
    let pb = DataProgressReporter::new("Testing progress bar (no totals, no bytes)", None, None);

    pb.register_progress(Some(1), None);
    sleep(time::Duration::from_millis(500));
    pb.register_progress(Some(2), None);
    sleep(time::Duration::from_millis(500));
    pb.register_progress(Some(1), None);
    sleep(time::Duration::from_millis(500));
    pb.finalize();

    let pb = DataProgressReporter::new("Testing progress bar (total count, no bytes)", Some(5), None);
    pb.register_progress(Some(1), None);
    sleep(time::Duration::from_millis(500));
    pb.register_progress(Some(2), None);
    sleep(time::Duration::from_millis(500));
    pb.register_progress(Some(1), None);
    sleep(time::Duration::from_millis(500));
    pb.finalize();

    let pb = DataProgressReporter::new("Testing progress bar (no totals, only bytes)", None, None);

    pb.register_progress(None, Some(5000));
    sleep(time::Duration::from_millis(500));
    pb.register_progress(None, Some(6000));
    sleep(time::Duration::from_millis(500));
    pb.register_progress(None, Some(4000));
    sleep(time::Duration::from_millis(500));
    pb.finalize();

    let pb = DataProgressReporter::new("Testing progress bar (only bytes + total)", None, Some(20000));

    pb.register_progress(None, Some(5000));
    sleep(time::Duration::from_millis(500));
    pb.register_progress(None, Some(6000));
    sleep(time::Duration::from_millis(500));
    pb.register_progress(None, Some(4000));
    sleep(time::Duration::from_millis(500));
    pb.finalize();

    let pb = DataProgressReporter::new("Testing progress bar (no totals, both count + bytes)", None, None);

    pb.register_progress(Some(5), Some(5000));
    sleep(time::Duration::from_millis(500));
    pb.register_progress(Some(10), Some(6000));
    sleep(time::Duration::from_millis(500));
    pb.register_progress(Some(12), Some(4000));
    sleep(time::Duration::from_millis(500));
    pb.finalize();

    let pb = DataProgressReporter::new("Testing progress bar (Total count)", Some(30), None);

    pb.register_progress(Some(5), Some(5000));
    sleep(time::Duration::from_millis(500));
    pb.register_progress(Some(10), Some(6000));
    sleep(time::Duration::from_millis(500));
    pb.register_progress(Some(12), Some(4000));
    sleep(time::Duration::from_millis(500));
    pb.finalize();

    let pb = DataProgressReporter::new("Testing progress bar (Total count + total bytes)", Some(30), Some(20000));

    pb.register_progress(Some(5), Some(5000));
    sleep(time::Duration::from_millis(500));
    pb.register_progress(Some(10), Some(6000));
    sleep(time::Duration::from_millis(500));
    pb.register_progress(Some(12), Some(4000));
    sleep(time::Duration::from_millis(500));
    pb.finalize();
}
