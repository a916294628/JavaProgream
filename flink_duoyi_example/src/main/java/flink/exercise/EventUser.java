package flink.exercise;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventUser {
    private Integer id;
    private String eventId;
    private Integer num;
    private String gender;
    private String city;
}
