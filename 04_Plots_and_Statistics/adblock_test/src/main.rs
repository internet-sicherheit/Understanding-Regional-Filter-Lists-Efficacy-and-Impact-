use adblock::{Engine, lists::{FilterSet, ParseOptions}, request::Request};
use serde_json::{json, Value, to_string_pretty};
use std::{fs, io::{self, BufRead, BufReader}, collections::HashMap, sync::{Arc, Mutex}};
use csv::Reader;
use threadpool::ThreadPool;
use std::time::Instant;

fn main() -> io::Result<()> {
    let filter_list_files = Arc::new(vec![
        "USA.txt", "VAE.txt", "China.txt", "France.txt", "Germany.txt", "Indian.txt",
        "Israel.txt", "Japanese.txt", "Scandinavia.txt"
    ]);

    //let url_list = Arc::new(vec![]);
    let path = "CSV/";

    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries{
            if let Ok(entry) = entry {
                if let Some(file_name) = entry.file_name().to_str() {
                    println!("{}", file_name);
                    let number = &file_name[file_name.len() - 3..];

                    let mut rdr = Reader::from_path(path.to_string()+file_name)?;
                    let urls_to_check: Vec<String> = rdr.records()
                    .filter_map(Result::ok)
                    .map(|r| r.get(0).unwrap_or_default().to_string())
                    .collect();

                    let pool = ThreadPool::new(410); // Create a thread pool with 10 threads
                    let results = Arc::new(Mutex::new(Vec::new()));
                    let start_time = Instant::now();

                    for url in urls_to_check {
                        let results = Arc::clone(&results);
                        let filter_list_files = Arc::clone(&filter_list_files);
                        pool.execute(move || {
                            let mut result_for_url: HashMap<String, Value> = HashMap::new();
                            result_for_url.insert("url".to_string(), json!(&url));

                            for file_name in &*filter_list_files {
                                let file_path = format!("./{}", file_name);
                                let file = fs::File::open(&file_path).unwrap();
                                let reader = BufReader::new(file);
                                let rules: Vec<String> = reader.lines().collect::<Result<_, _>>().unwrap();
                                let mut filter_set = FilterSet::new(true);
                                filter_set.add_filters(&rules, ParseOptions::default());
                                let engine = Engine::from_filter_set(filter_set, true);

                                let request = Request::new(&url, "", "").unwrap();
                                let blocker_result = engine.check_network_request(&request);
                                let base = file_name.split('.').next().unwrap();

                                result_for_url.insert(format!("filterlist_{}_is_blocked", base), json!(blocker_result.matched));
                                result_for_url.insert(format!("filterlist_{}_matched_rule", base), json!(blocker_result.filter.as_ref().map_or("", |f| &**f)));
                            }

                            let mut res = results.lock().unwrap();
                            res.push(json!(result_for_url));
                        });
                    }

                    pool.join(); // Wait for all tasks in the pool to complete

                    let total_duration = start_time.elapsed().as_secs_f64();
                    println!("Total processing time: {:.2} seconds", total_duration);

                    let results = Arc::try_unwrap(results).unwrap().into_inner().unwrap();
                    let json_data = to_string_pretty(&json!(results))?;
                    fs::write(number.to_string() + "results.json", json_data)?;
                    
                    
                }
            }
        }
    
    } 
    Ok(())
}
