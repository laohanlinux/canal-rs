pub struct Client {

}

impl Client{
    pub fn get_with_out_ack(batch_size: usize, timeout: Option<i64>, uints: Option<i32>) {

    }

    pub fn get(batch_size: usize, timeout: Option<i64>, uints: Option<i64>) {

    }

    pub fn subscribe(filter: &String) -> Result<(), String> {
        Ok(())
    }

    pub fn unsubscribe() -> Result<(), String> {
        Ok(())
    }

    pub fn ack(batch_id: i64) -> Result<(), String> {
        Ok(())
    }

    pub fn roll_back(batch_id: i64) -> Result<(), String> {
        Ok(())
    }

    pub fn write_header() {}
}