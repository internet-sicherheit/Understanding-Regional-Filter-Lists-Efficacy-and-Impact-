use adblock::{Engine, lists::{FilterSet, ParseOptions}, request::Request};
use serde_json::{json, Value, to_string_pretty};
use std::{fs, io::{self, BufRead, BufReader, BufWriter}, collections::HashMap, sync::{mpsc, Arc, Mutex}};
use csv::Reader;
use threadpool::ThreadPool;
use std::time::{Instant, Duration};
use std::fs::File;
use std::path::Path;
use std::collections::HashSet;
use memory_stats::memory_stats;
use sysinfo::{System, SystemExt};
use rand::seq::SliceRandom;
use rand::thread_rng;
use indicatif::{ProgressBar, ProgressStyle};


fn main() -> io::Result<()>{
    let filter_list_files = [
        "USA_sanitized.txt", "VAE_sanitized.txt", "China_sanitized.txt", "France_sanitized.txt", "Germany_sanitized.txt", "Indian_sanitized.txt",
        "Israel_sanitized.txt", "Japanese_sanitized.txt", "Scandinavia_sanitized.txt"
    ];

    let mut unique_rules = HashSet::new();

    for filter_list in &filter_list_files {
        if let Ok(file) = File::open(filter_list) {
            let reader = BufReader::new(file);
            for line in reader.lines() {
                if let Ok(line) = line {
                    unique_rules.insert(line);
                }
            }
        } else {
            eprintln!("Fehler beim Öffnen der Datei: {}", filter_list);
        }
    }

    let path = "/home/ubuntu/Desktop/filterlists/CSV/";
    let urls = "urls.csv";

    println!("Unique rules: {}",unique_rules.len());

    let mut rdr = Reader::from_path(path.to_string()+urls)?;
    //let urls_to_check: Vec<String> = rdr.records()
    //.filter_map(Result::ok)
    //.map(|r| r.get(0).unwrap_or_default().to_string())
    //.collect();

    let mut system = System::new_all();
    let n_threads = 4;
    let n_urls = 100_000;
    let urls_to_check: Vec<String> = rdr.records()
        .filter_map(Result::ok)
        .map(|r| r.get(0).unwrap_or_default().to_string())
        .take(n_urls)
        .collect();


    println!("URLs to check: {}", urls_to_check.len());

    let mut evaluation_loop = 300;
    let mut i = 1;

    let pb_ev = ProgressBar::new(100);
    pb_ev.set_style(ProgressStyle::default_bar()
        .template("{msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
        .progress_chars("#>-"));

    while i <= evaluation_loop {

        // Round one
        let mut num_entries_to_write = unique_rules.len();
        let mut round = 1;

        // 143.655
        // 5 * 28.731

        let pb_while = ProgressBar::new(5);
        pb_while.set_style(ProgressStyle::default_bar()
            .template("{msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .progress_chars("#>-"));

        // Multi producer single consumer FIFO queue
        // One tx clone per thread feeding into a single rx
        let (tx, rx) = mpsc::channel();
        let pool = ThreadPool::new(n_threads);

        while round <= 5 {
            let mut total_memory = 0;

            let n_urls_per_thread: usize = n_urls / n_threads;

            let unique_rules_vec: Vec<&String> = unique_rules.iter().collect();

            let mut rng = thread_rng();
            let rules: Vec<String> = unique_rules_vec
                .choose_multiple(&mut rng, num_entries_to_write)
                .cloned()
                .cloned()
                .collect();

            println!("Rules to check: {}", rules.len());

            let start_time = Instant::now();

            let pb_for = ProgressBar::new(n_urls as u64);
            pb_for.set_style(ProgressStyle::default_bar()
                .template("{msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
                .progress_chars("#>-"));


            for x in 0..n_threads {
                let url_offset = x * n_urls_per_thread;
                let mut urls = Vec::new();
                urls_to_check[url_offset..url_offset+n_urls_per_thread].clone_into(&mut urls);
                let thread_tx = tx.clone();
                let rules = rules.clone();
                let pb_for = pb_for.clone();

                pool.execute(move || {
                    let mut filter_set = FilterSet::new(true);
                    filter_set.add_filters(&rules, ParseOptions::default());
                    let engine = Engine::from_filter_set(filter_set, true);

                    let mut result_for_url: HashMap<String, i32> = HashMap::new();

                    for url in urls {
                        let request = Request::new(&url, "", "").unwrap();
                        let blocker_result = engine.check_network_request(&request);
                        let rule = blocker_result.filter.as_ref().map_or("", |f| &**f);
                        result_for_url.insert(rule.to_string(), 0);

                        if blocker_result.matched {
                            let c = result_for_url.get(rule);
                            let new_v = c.unwrap()+1;
                            result_for_url.insert(rule.to_string(), new_v);
                        }

                        thread_tx.send(result_for_url.clone())
                            .expect("msg sent");

                        pb_for.inc(1);
                    }
                });
            }

            pool.join();

            system.refresh_all();
            let average_memory = system.used_memory();
            let duration = start_time.elapsed();

            let results: HashMap<String, i32> = rx.try_iter()
                .fold(HashMap::new(), |mut acc, x| {
                    for (k,v) in x {
                        *acc.entry(k.to_owned()).or_default() += v;
                    }
                    acc
                });

            println!("Checked {} rules", results.len());

            let json_data = json!({
                "Rounds": round,
                "Entries": num_entries_to_write,
                "Duration": duration.as_secs(), // IN SEC
                //"URLs": urls_to_check,
                "Memory_used": average_memory, // IN KB
                "Results": results,
            });

            let writer = BufWriter::new(File::create("/home/ubuntu/Desktop/filterlists/2024-filterlists/runtime/results/".to_string()+&i.to_string() + "_" + &round.to_string() + "_results.json").unwrap());
            serde_json::to_writer_pretty(writer, &json_data).ok();

            num_entries_to_write -= 28_731;
            round += 1;
            pb_while.inc(1);
        }
        i += 1;
        pb_ev.inc(1);
    }

    Ok(())
}
