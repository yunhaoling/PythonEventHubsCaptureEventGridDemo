class WindTurbineMeasure:
    def __init__(self, device_id, measture_time, generated_power, wind_speed, turbine_speed):
        self.device_id = device_id
        self.measture_time = measture_time
        self.generated_power = generated_power
        self.wind_speed = wind_speed
        self.turbine_speed = turbine_speed

    def to_dict(self):
        return {
            "DeviceId" : self.device_id,
            "MeasureTime" : str(self.measture_time)[:-3],
            "GeneratedPower" : self.generated_power,
            "WindSpeed" : self.wind_speed,
            "TurbineSpeed" : self.turbine_speed
        }
        