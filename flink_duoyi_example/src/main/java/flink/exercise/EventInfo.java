package flink.exercise;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EventInfo {
    private Integer id;
    private String eventId;
    private Integer cut;
}
